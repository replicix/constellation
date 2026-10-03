# Plan 37 — Kubernetes CSI driver

Read `docs/plans/v1/CONVENTIONS.md` first. This plan turns Constellation
into a native Kubernetes storage backend: a CSI driver where a
`PersistentVolume` is a Constellation `View`, `ReadWriteMany` is a first-class
access mode (not a bolted-on NFS re-export), and snapshots/clones are
Constellation snapshots under a plan-32 hold. Linux only.

Depends on **plan 31 (`31-core-frontend-backend.md`)**, specifically:

- **C4** (`constellation-vfs`, the response-barrier contract) plus the CSI
  seam this plan requires there: `MountSource::{Path, PreopenedFd(OwnedFd)}`,
  `FuseSession::detach() -> SessionHandoff`, `FuseSession::resume(SessionHandoff)`,
  `View::export_handles() -> HandleTableSnapshot`,
  `Engine::open_view_resumed(spec, HandleTableSnapshot, ...)`. This is the
  FUSE session handover machinery K0/K5 below build on.
- **C5** (the control protocol: framing, handshake, `UnixSocket`/`InProcess`
  transports, roles, audit) plus its CSI seam: `Transport::send_fd`
  (SCM_RIGHTS over `UnixSocket`; `InProcess` passes the fd directly;
  `NamedPipe` does not support it and is irrelevant here — Linux only), the
  control methods this plan drives (`fs.create`, `fs.unlock`, `view.mount`,
  `view.unmount`, `view.list{labels}`, `view.stats`, `quota.set`,
  `quota.get`, `snapshot.create{hold}`, `snapshot.delete`, `clone.create`,
  `node.leave`, `node.handoff`, and plan 31's `browse.*` methods
  (`browse.mkdir`, `browse.rename`, `browse.readdir`, `browse.delete`,
  `browse.xattr`) that the volume-pool design (§2.3, §4) uses), and
  `EphemeralSecretStore` and per-engine `CredentialSource`.
- **C8** (`EngineProfile`, in particular the `Server` preset for dense
  multi-PV hosts) and `EngineHost` (one process hosting N `Engine`s sharing a
  `ResourceBudget`) — the engine pod is an `EngineHost` with N = 1.
- **`platform::linux::fuse_mount_fd`** — the privileged, fusermount3-free
  mount helper that returns the fd, shared between the plain Linux daemon
  and this plan's node plugin.
- **`ViewSpec.labels: BTreeMap<String,String>`** surfacing in `view.list`,
  `OpWatch`, tracing spans and metrics with a bounded cardinality allowlist.

None of this exists in `crates/cli` today (VERIFIED against `a945b05`: no
`crates/csi`, no `fuse_mount_fd`, no `SessionHandoff`, no `EngineHost`) —
plan 31 is being written in parallel to carry these seams, and this plan's
milestones assume that target shape, not today's monolith. Every fixed name
above is quoted verbatim from the shared design brief, except `fs.create`
and the `browse.*` family, which this plan's later volume-pool revision
(§2.3) introduces as new, explicitly-flagged additions plan 31's session
needs to pick up — this plan does not rename or relitigate any of the
brief's own original fixed names.

Also depends on **plan 33 (`33-control-plane-and-ui.md`), U1 only**: roles
(`viewer < operator < admin`), the `control-acl.toml` allowlist format, and
the audit log. This plan adds one new grant `kind` to that format (§"Service
principal identity" below) — a targeted addition to U1, not a redesign of it.

**Coordinates with plan 32** (snapshot policies and space accounting, owned
by another session): this plan's `CreateSnapshot`/`DeleteSnapshot` need
plan 32's hold-owner-namespace work (`csi:` holds that policy pruning never
touches) and its space-accounting fields for `size_bytes`. This plan does
not edit plan 32; §"Snapshots and clones" records exactly what it needs from
it, for plan 32's own session to pick up.

**Independent of plans 34, 35 and 36** (macOS/Windows/Android ports) — this
plan is Linux-only; the engine pod is a Linux container image regardless of
what OS a desktop Constellation build runs on, and nothing here touches the
NFS, WinFsp or SAF frontends.

The research behind this plan was done on 2026-09-28, the same day as plan
31's. **VERIFIED** means checked this session against the upstream source
(the CSI spec repo, `kubernetes-csi` org repos, `fuser` 0.18.0's vendored
source at `~/.cargo/registry/.../fuser-0.18.0`, official docs, or a `gh
api`/`gh release` call). **REPORTED** means a secondary source (blog posts,
search-engine summaries) not independently re-derived from primary source in
this session — K0 re-checks the load-bearing REPORTED claims (the fuser
INIT-skip patch shape) before anything is built on them. Sources are listed
at the end.

## 1. Why

Constellation's core pitch — a POSIX filesystem on S3 with a working-set
cache and P2P fan-out, not an object-store facade — is a materially
different Kubernetes storage story than the two things people reach for
today:

- **EBS/PD-backed CSI drivers** give `ReadWriteOnce` block storage:
  fast, but exactly one node, no sharing, no S3 durability model.
- **EFS/Filestore-style NFS CSI drivers** give real `ReadWriteMany`, but pay
  every read from the network file server; there is no node-local working
  set, no P2P cache fan-out between pods on different nodes reading the same
  hot data, and durability is whatever the managed NFS service offers, not
  S3's.

Constellation's engine already does the hard part — leases, coop cache,
epoch-fenced writes, snapshots, prune — Linux-side, over FUSE. The gap is
purely integration: Kubernetes needs a *driver*, not a filesystem, and a
driver has a specific shape (gRPC services, sidecars, a `CSIDriver` object)
that a hand-rolled FUSE daemon does not speak. Closing that gap turns every
existing Constellation capability (RWX, snapshots, quotas, P2P cache) into
something a `StorageClass` and a `PersistentVolumeClaim` can reach, which is
the on-ramp most platform teams actually use to adopt a new storage system.

The second problem this plan has to solve, distinct from the FUSE-churn
problem below, is **who owns a Constellation filesystem's lifecycle**. A
naive mapping — one Constellation filesystem per `PersistentVolume` — makes
every CSI driver operation trivial (`CreateVolume` = `fs.create`,
`DeleteVolume` = drop the filesystem) but pushes real costs onto the
*user*: a cluster provisioning hundreds of small PVs would need hundreds of
Constellation filesystems, each with its own idle-tailer S3 polling cost
(plan 26's finding — see §2.3), its own cache with nothing shared across
PVs that are obviously related (the same team's scratch volumes), and no
way to get a cheap CoW clone between two PVs, because Constellation's
clones and snapshots are subtree-granular *within one filesystem* (plan 09
§Step 4; `crates/api/src/types.rs`'s `SnapshotCreate{selector}`/
`Clone{selector, destination}` both take a subtree selector, not a
cross-filesystem one) — chunks live under one filesystem's own bucket
prefix, so a clone can't cheaply reference another filesystem's chunks.
This plan's answer is **volume pools** (§2.3): a `StorageClass` names only a
`bucket` (and an optional `prefix`), the driver owns everything under it —
creating the shared pool filesystem idempotently, carving out one subtree
per PV, tracking ownership in xattrs instead of a side database, and
trashing-then-purging deleted volumes asynchronously — so a user requesting
storage never has to pre-create or name a Constellation filesystem by hand.
Tenants that need per-volume isolation (their own E2E key, their own
failure domain) opt into `layout: dedicated` instead, at the cost this
plan's pool design exists specifically to avoid paying by default.

The third, and hardest, new problem this plan introduces is
**FUSE-in-Kubernetes churn**: a kernel FUSE mount is pinned to the process
that opened `/dev/fuse`. Every existing FUSE-backed CSI driver either (a)
runs the FUSE process inside the *node plugin* container, which means a
driver upgrade/crash/OOM kills every mount on that node (`ENOTCONN`,
"Transport endpoint is not connected", surfaced to every pod using the
filesystem), or (b) accepts that cost as a known limitation. Constellation's
engine already separates cleanly from its FUSE adapter (plan 31 C4's `Vfs`
contract) and already treats FUSE session lifetime as something the engine,
not the frontend, should own (`kernel_inval.rs`'s notifier thread, the
responder completion pool) — so this plan can do better: put the
FUSE-holding process in its own long-lived pod, decoupled from the CSI node
plugin's own lifecycle, adopt the same drain discipline every production
FUSE CSI driver already relies on (§2.2, always on, regardless of what
follows), and go one step further with **session handover** on top of it so
that even a *planned* engine-pod replacement doesn't interrupt writers. This
mirrors precedent (Sources table, the Mountpoint-for-Amazon-S3 and JuiceFS
CSI driver rows) from two production FUSE CSI drivers that independently
arrived at "FUSE process in its own pod, fd handed over the unix socket" —
Mountpoint for Amazon S3 CSI driver v2 and the JuiceFS CSI driver's
mount-pod mode — and pushes one step further than both by making the
*handover itself* lossless for open file handles, not just for the mount
point. §2.2 below is now VERIFIED against Mountpoint's own code (not just
its docs) that no such handover exists anywhere in that ecosystem — this is
genuinely new ground, and K0 (§15) treats it accordingly.

## 2. Decisions: FUSE session placement and volume pooling

### 2.1 Where does the FUSE session live?

| Option | Node-plugin crash blast radius | Engine-pod upgrade blast radius | Shares engine/cache across PVs of one fs | Extra moving parts | Verdict |
|---|---|---|---|---|---|
| **FUSE in the node plugin container itself** (classic pattern: most first-generation FUSE CSI drivers, e.g. early Mountpoint-S3 CSI v1, most `goofys`/`s3fs` sidecar drivers) | Every mount on the node dies (`ENOTCONN`) on every node-plugin restart, including routine driver upgrades and `livenessprobe`-triggered restarts | n/a (same process) | No — one process per node total, no per-fs isolation, one noisy-neighbour PV can starve every other PV's cache | Fewest — single privileged DaemonSet pod | **Rejected.** Kubernetes restarts DaemonSet pods routinely (upgrades, node pressure, `livenessprobe`); making every restart a filesystem outage for every pod on the node is not acceptable for what is supposed to be Constellation's *strongest* multi-writer story. |
| **One engine (and FUSE session) per PV** | A crash only affects that PV's pods | An upgrade of one PV's engine doesn't affect others, but N PVs on a node means N processes, N caches, N sets of leases/P2P listeners for what might be the *same* filesystem | No — defeats the purpose of a shared working-set cache across PVs of one filesystem on one node | Most — the pod count scales with PV count, not node count or filesystem count | **Rejected.** Constellation's value is the shared cache and lease/coop machinery; splitting it per PV means two pods on the same node mounting the same filesystem's two different subtrees can't share a cache page, can't share a lease, and pay S3/P2P costs twice. It also multiplies `EngineProfile` memory/cache budgets by PV count instead of by filesystem count. |
| **One engine pod per (Constellation filesystem, k8s node), FUSE fd handed off from the node plugin** — **chosen** | A crash affects only PVs of that one filesystem on that one node; other filesystems and other nodes are unaffected | An upgrade of one engine pod affects only that filesystem's PVs on that node, and with session handover (§8) not even that, for in-flight I/O | Yes — `View`s for every PV of that filesystem on that node share one `Engine`'s cache, leases and P2P endpoint | Moderate — pod count scales with distinct (filesystem, node) pairs actually in use, which is the natural unit of resource sharing | **Chosen.** Matches the natural sharing boundary (one filesystem = one working set, one lease namespace, one P2P identity per node) while keeping blast radius at exactly that boundary, and lets the *unprivileged* engine pod do all the engine work while only the *privileged* node plugin ever calls `mount(2)`. |
| **Kernel NFS re-export**: a Constellation-owned NFS server (plan 34's `frontend-nfs`, in-cluster) fronting the engine, mounted by kubelet via the built-in Linux NFS client, no FUSE in the picture at all | A server pod restart is a normal NFSv4.1 reconnect (grace period), not `ENOTCONN` — genuinely more resilient to restarts than raw FUSE | Same resilience story as the crash case — NFSv4.1 clients recover across a server restart within the grace period | Yes, same sharing boundary as the chosen option if scoped per filesystem | Fewer new primitives *in Kubernetes* (no fd passing, no session handover, no privileged node plugin beyond the kernel NFS client's own mount call) but a large new one *in Constellation* — plan 34's NFSv4.1 server has to exist and be hardened first | **Rejected for this plan, recorded as the honest runner-up.** It trades this plan's hardest problem (FUSE session handover) for a dependency this plan cannot assume: plan 34's NFS frontend, built and hardened for a *desktop* macOS client, would need to become a multi-tenant, quota-enforcing, label-aware server fronting arbitrary numbers of Kubernetes-mounted filesystems — materially more surface than "expose one already-open FUSE fd to a sibling pod." It also reintroduces exactly the network round-trip on every read that FUSE-local-then-S3-cold-then-P2P avoids: an in-cluster NFS re-export sits *between* the pod and the engine's cache, whereas the FUSE-fd-in-engine-pod design puts the engine's cache in the same failure/latency domain as the FUSE mount, one hop closer to the workload. If K0's fd-passing/handover spike turns out to be infeasible even after the pre-agreed fallbacks (§15's K0 gate), this is the documented plan B, not a stopgap improvised under pressure. |

### 2.2 Compared with Mountpoint for Amazon S3's CSI driver

This session re-read Mountpoint's own source (`mountpoint-s3`
`e144a7bb84948045f0d7cde77060afaa7ed91b53` and `mountpoint-s3-csi-driver`
`b450b22beae8bddc0d3c09551655b2b7d323e29b`, both `main`, 2026-09-28 — see the
Sources table) rather than relying on its docs alone. Three findings change
this plan from "argued by analogy" to "checked against code":

- **Positioning.** Mountpoint is deliberately, explicitly **not** a POSIX
  filesystem (VERIFIED, `mountpoint-s3/doc/SEMANTICS.md`): sequential
  single-writer whole-file writes only, no in-place edits, rename supported
  only on S3 Express One Zone, no locks, no hard/symlinks, and consistency
  that degrades from close-to-open (no cache) to a metadata TTL (with
  caching enabled) rather than staying strict. Constellation's full POSIX
  surface, leases and advisory locks, coop cache, offline epochs and
  snapshot/clone model are a *materially larger scope* than Mountpoint ever
  attempts — worth stating plainly in this plan's own docs (§"Semantic
  notes") rather than only implying it, so a reader who knows Mountpoint
  does not undersell what RWX means here.
- **fd-passing mechanics: identical, and now VERIFIED against code, not
  just docs.** `mountpoint-s3-csi-driver`'s CSI Node component
  (`pkg/driver/node/mounter/pod_mounter.go`) opens `/dev/fuse` itself, calls
  `mount(2)` on a node-local source path, sends the fd to the Mountpoint Pod
  over a Unix socket via `SCM_RIGHTS`
  (`pkg/mountpoint/mountoptions/mount_options.go`:
  `syscall.UnixRights`/`WriteMsgUnix`), then **closes its own copy of the
  fd** so exactly one process owns it from that point on
  (`pod_mounter.go`'s `defer pm.closeFUSEDevFD(...)`), commit
  `b450b22beae8bddc0d3c09551655b2b7d323e29b`. This is line-for-line the same
  sequence as `fuse_mount_fd`/`Transport::send_fd` and settled decision 5's
  staging/bind-mount split — strong, working, production-hardened precedent
  for this plan's design, not a novel invention on that front. This plan
  adopts the same "close the parent's copy immediately after the
  `SCM_RIGHTS` send" discipline explicitly, at every relay step (§8 step 4
  included), for the same reason Mountpoint needs it: unambiguous fd
  ownership.
- **Pod sharing is narrower than this plan's, and for a reason that doesn't
  apply to Constellation.** Mountpoint shares a Mountpoint Pod only across
  workload pods mounting the *same volume ID*
  (VERIFIED, `pkg/api/v2/mountpoints3podattachment_types.go`'s
  `MountpointS3PodAttachmentSpec{NodeName, VolumeID}`,
  `docs/MOUNTPOINT_POD_SHARING.md`) — it has no cross-PV shared cache,
  lease or P2P concept to make coarser sharing valuable, so there was never
  a "share across every PV of one bucket" option on the table for
  Mountpoint to reject. This plan's per-(pool, node) engine-pod sharing
  (§2.1, and §2.3 below for pool vs. dedicated) is *justified by* the
  working-set cache, coop cache and lease/roster machinery Mountpoint
  simply does not have — the comparison is real, not a difference Mountpoint
  merely declined to make.
- **Session handover has no precedent anywhere in this ecosystem — VERIFIED
  by reading the only two `fuser`-fork constructors that exist, not by their
  absence from the docs.** `mountpoint-s3-fuser`'s own fork moved `INIT`
  handling into the ordinary per-request dispatch loop instead of a
  blocking pre-loop (`request.rs:124-161`), but its `Session::from_fd`
  still sets `initialized: AtomicBool::new(false)` (`session.rs:132`) and is
  **only ever called against a freshly `mount(2)`-ed connection** — every
  call site in the CSI driver follows a brand-new mount in the same code
  path (`pod_mounter.go`'s `mountS3AtSource`), never against a live
  connection inherited from a dead sibling process. Mountpoint CSI v2's own
  `RestartPolicy: OnFailure` Pod spec means a Mountpoint Pod crash or
  restart is accepted as a genuine `ENOTCONN` outage
  (`docs/TROUBLESHOOTING.md` documents this as a trade-off, not a bug),
  recovered only by redoing the entire open+mount(2)+`SCM_RIGHTS` cycle
  against a fresh kernel connection. **This means plan 37's
  `FuseSession::detach()`/`resume()` + `node.handoff` protocol (§8) has no
  working reference implementation in Mountpoint, JuiceFS, or anywhere else
  surfaced by this research — it is first-of-its-kind, not an adaptation of
  an existing technique.** K0 is this plan's single highest-risk item for
  exactly that reason, and is proven first, before K1-K7 assume it works.
- **Adopt Mountpoint's drain discipline as this plan's always-on baseline,
  independent of whatever K0 finds.** Mountpoint's only mitigation for the
  *planned* replacement case — since it has no handover — is: the
  Mountpoint Pod ignores `SIGTERM` while it is serving mounts, carries a
  10-minute `terminationGracePeriodSeconds`, and the driver documents
  draining workload pods before the Mountpoint Pod is force-killed
  (`docs/TROUBLESHOOTING.md`, `pkg/podmounter/mppod/creator.go`'s
  `TerminationGracePeriodSeconds = 600`). This plan adopts exactly that
  baseline for engine pods (§8's Drain subsection, §8's failure handling),
  *unconditionally* — not as a K0-failure fallback, but as the safety net
  every engine pod always has, the same way Mountpoint always has it: an
  engine-pod crash (OOM, panic — not a graceful `node.handoff`) is `ENOTCONN`
  until the node plugin notices and remounts, triggered by
  `requiresRepublish: true` (settled decision 12, revised from this plan's
  earlier `false` — see there for why). Session handover (§8, gated on K0)
  is the *upgrade path layered on top* of that baseline for the planned
  case, not a replacement for it — if K0 fails, this plan still ships with
  exactly Mountpoint's own safety net, never with nothing.

### 2.3 Volume-to-filesystem mapping: pool vs. dedicated vs. filesystem-per-PV

This is a separate decision from §2.1 (which process holds the FUSE
session): §2.1 fixes *where the engine runs*; this decision fixes *what
Constellation filesystem a PV's subtree lives in*, which is what the
`StorageClass` actually configures (`layout` parameter, §"StorageClass
parameters"). Facts this rests on, verified in the repo and restated from
the coordinator brief:

- Constellation's snapshots and clones are **subtree-granular and
  copy-on-write inside one filesystem only** (plan 09 §Step 4; `crates/api/
  src/types.rs:93-110`'s `SnapshotCreate{selector}`/`Clone{selector,
  destination}` both take a same-filesystem subtree selector). Chunks live
  under one filesystem's own bucket prefix, so a clone cannot cheaply
  reference another filesystem's chunks — a cross-filesystem clone is a
  full data copy, not a metadata-only operation.
- Each filesystem pays its own idle-metadata-polling cost per node (plan
  26's finding: per-node idle metadata polling is the dominant steady-state
  S3 cost) — one tailer per filesystem per node, regardless of how many PVs
  that filesystem holds.
- A shared filesystem shares its working-set cache, coop-cache fan-out and
  content-addressed chunk store across every PV inside it — a second PV
  that happens to hold duplicate data (the same base image seeded into two
  scratch volumes, say) dedups for free; two PVs in two different
  filesystems never share a chunk.

| Option | CoW snapshots/clones | Idle tailer cost | Shared cache/dedup | Isolation | Verdict |
|---|---|---|---|---|---|
| **Pool** (`layout: pool`, **default**): one shared Constellation filesystem per `StorageClass`, auto-created from just `bucket`/`prefix`; every PV is a subtree `/volumes/<pv-name>` inside it | Free and cheap between any two PVs of the same pool (same-filesystem subtree clone) | One tailer per (pool [shard], node) — shared across every PV of that pool on that node | Yes — one cache, one lease namespace, one dedup domain per pool | Weaker: one E2E key, one metadata commit chain, one GC domain for the whole pool; the engine pod can reach every PV in the pool, not just the caller's own (§"Credentials and security" states this trust model explicitly) | **Chosen as the default.** Matches "user supplies only a bucket" (§1): no out-of-band filesystem creation, and the common case (many related PVs — a team's namespace, a CI system's ephemeral scratch volumes) gets free clones and a shared cache for free. |
| **Dedicated** (`layout: dedicated`): one Constellation filesystem per PV, at `prefix/<pv-name>/` | Only against another dedicated volume's *own* snapshots — a clone from one dedicated PV's snapshot into a new dedicated PV is a full copy (server-side `CopyObject` of chunks + metadata import) or unsupported (this plan: unsupported at K0-K7, a documented later milestone) | One tailer per PV per node — the worst case, paid deliberately | No — by design; that is the isolation this option buys | Strongest available from this driver: independent E2E key, independent failure domain, independent GC; a compromised or buggy tenant's engine pod never has a live connection to another tenant's data | **Kept, opt-in.** For tenants whose compliance or blast-radius requirements outweigh the pool's efficiency — a `StorageClass` per tenant, `layout: dedicated`, same driver, no separate code path for "isolated mode" beyond this one parameter. |
| **Filesystem-per-PV as the *only* option** (no pooling at all — the naive default a first cut of this driver would ship, and in effect what this plan's own earlier draft did by requiring a StorageClass to name one pre-existing Constellation filesystem and mapping every PV to a subtree of it, with no xattr-based ownership or trash) | Same as dedicated — no cross-PV sharing exists to clone from | Same as dedicated — paid by every user, not just tenants who need it | None, ever | Same as dedicated, whether or not any given tenant wants it | **Rejected as the only mode.** Forcing every deployment to pay dedicated's idle-tailer and no-shared-cache cost, and to hand-manage or explicitly name a Constellation filesystem before provisioning anything, directly contradicts §1's "user supplies only a bucket" goal for the overwhelmingly common case where PVs in one `StorageClass` *are* related enough to share a filesystem safely. Dedicated survives as an explicit, opt-in `layout` for the minority that needs it instead. |

**Shards.** Within `layout: pool`, `shards: N` (default 1) hashes PVs by
name onto `N` pool filesystems at `prefix/shard-<k>/`, to lift the
per-pool metadata throughput ceiling that the shared CAS commit chain
otherwise imposes as PV count grows on one filesystem. Clones and restores
stay within the source's shard (§"Settled decisions"). K0 (§15) measures
one pool filesystem's metadata-throughput ceiling under many PVs across
many nodes specifically to give operators concrete sharding guidance
("shard once you exceed N PVs per pool" or similar), rather than this plan
guessing a number up front.

## 3. Settled decisions — do not relitigate

1. **Crate `crates/csi` → binary `constellation-csi`.** It is a *management
   client* of the control protocol, exactly like the CLI, the harness and
   plan 33's UI — never a `Vfs` frontend, never linked against
   `constellation-engine` directly. It speaks gRPC (via `tonic`) to
   Kubernetes and the control protocol (via `constellation-control`'s client
   library, `UnixSocket` transport, `Transport::send_fd`) to engine pods.
2. **Driver name: `csi.constellation.dev`.** Used as `spec.driverName` in
   `CSIDriver`, the `provisioner`/`driver` fields of `StorageClass`/
   `VolumeSnapshotClass`, and `GetPluginInfo`'s `name`.
3. **Controller is a `Deployment` (replicas: 2, leader-election via the
   standard `external-provisioner`/`external-resizer`/`external-snapshotter`
   lease mechanism), Node is a `DaemonSet`.** Standard CSI topology; no
   Constellation-specific deviation.
4. **`ControllerPublishVolume`/`ControllerUnpublishVolume` are not
   implemented and not advertised.** `attachRequired: false` in `CSIDriver`
   — there is no "attach" step separate from mount for an S3-backed
   filesystem; every node can reach every filesystem's S3 bucket directly.
5. **Stage once per (PV, node); publish per pod via bind mount.** The driver
   advertises the `STAGE_UNSTAGE_VOLUME` node capability. `NodeStageVolume`
   is where the real FUSE work happens — `fuse_mount_fd` plus `view.mount`
   against a node-local *global staging path*
   `/var/lib/kubelet/plugins/csi.constellation.dev/staging/<pv-name>/globalmount`
   (naming convention shared with `csi-driver-nfs`'s
   `globalmount` staging layout — Sources table, `csi-driver-nfs` row).
   `<pv-name>` here is `req.volume_id`'s CSI-visible name (`pvc-<uid>`), not
   the pool-internal path (settled decision 7) — kubelet's staging-path
   convention is keyed by the volume it thinks it's staging, independent of
   which pool or shard that volume actually routes to. `NodePublishVolume` is
   then a plain kernel bind mount (`MS_BIND`, read-only for `ROX`) from the
   staging path to the pod's own `target_path`, with **no new FUSE session
   per pod**. This is the mechanism that lets N pods on one node share one
   PV's mount cheaply, and it is why §"CSI RPC mapping" below splits
   `view.mount` out of `NodePublishVolume` and into `NodeStageVolume` —
   a deliberate refinement of the design brief's shorthand ("NodePublishVolume
   = `view.mount`"), stated explicitly here because it changes which RPC
   does the expensive work and because the brief's own architecture section
   never mentions `NodeStageVolume` at all. `NodeUnstageVolume` is only
   called by kubelet after every `NodeUnpublishVolume` for that volume on
   that node has completed (a CSI spec guarantee, VERIFIED spec.md's
   `NodeStageVolume`/`NodeUnstageVolume` RPC description: "the SP SHALL
   assume this RPC will be executed regardless of the mount state on the
   node"; the *sequencing* guarantee is the standard sidecar/kubelet
   contract every staging-capable driver relies on) — that is the trigger
   for `view.unmount`.
6. **`NodeExpandVolume` is a no-op, and `EXPAND_VOLUME` is advertised
   controller-only.** Constellation's quota is metadata, not an on-disk
   filesystem size a kernel fs driver needs to grow; `ControllerExpandVolume`
   alone (a `quota.set` call) is sufficient, `node_expansion_required:
   false` in the `ControllerExpandVolumeResponse`.
7. **PV = View. Volume identity is self-describing, and the controller is
   stateless.** `volume_id = <pool-fs-uuid>/<path-within-pool>` (e.g.
   `4f9c…/volumes/pvc-1a2b`, or `4f9c…/shard-2/volumes/pvc-…` under
   sharding, or `<fs-uuid>/` — the whole filesystem root — for `layout:
   dedicated`). Encoding the pool's filesystem uuid *and* the subtree path
   into `volume_id` means `NodeStageVolume` (and every other Node/Controller
   RPC) can find the right engine pod and the right subtree from
   `req.volume_id` alone, with no separate ID-mapping table or database
   anywhere in this driver:
   - **`layout: pool` (default).** `CreateVolume`'s `req.Name`
     (`pvc-<uid>`, `external-provisioner`'s deterministic name) becomes the
     subtree `/volumes/<req.Name>/` inside the pool filesystem named by the
     `StorageClass`'s `bucket`/`prefix` (and `shard-<k>/` if `shards > 1`,
     chosen by hashing `req.Name`). **Volume records are xattrs on the
     volume directory**: `user.constellation.csi.{pv,pvc,namespace,
     capacity,source,created}` — no CRD, no sidecar database. `CreateVolume`
     is idempotent by comparing an existing directory's xattrs against the
     request: a match returns `OK`, a mismatch (same name, different
     `capacity`/`source`/owner) returns `ALREADY_EXISTS`. Deleted volumes
     move to `/.trash/<pv-name>-<deleted-ts>/` rather than being deleted
     synchronously (§"Deletion and purge"). Static provisioning names any
     existing path inside the pool directly as the `volumeHandle`. Because
     every fact `ListVolumes`, idempotency checks and GC need (ownership,
     capacity, trash state) lives in the filesystem itself as directory
     entries and xattrs, **the controller holds no volume state of its
     own** — a controller replica can restart, fail over, or run as either
     of its 2 leader-elected replicas, and every RPC re-derives its answer
     by reading the pool, not a cache.
   - **`layout: dedicated`.** `req.Name` names an entire Constellation
     filesystem at `prefix/<req.Name>/` instead of a subtree of a shared
     one; `volume_id` is just that filesystem's uuid (no
     `/path-within-pool` suffix — the "subtree" is the filesystem root).
     `DeleteVolume` drops the whole filesystem (a batch-delete job, not a
     trash rename — there is no shared pool to protect from a mistaken
     trash-purge sweep, so immediate deletion is safe and simpler here).
   - Both layouts make every controller RPC idempotent on `req.name`
     (create) or `req.volume_id` (everything else) — §"CSI RPC mapping"
     column 3 — the same property the plan's earlier "use `req.Name`
     verbatim" design had, just re-derived from richer, self-describing
     identifiers instead of a bare name.
8. **Snapshots and clones use plan 32's holds; this plan does not implement
   its own snapshot storage.** `CreateSnapshot` = `snapshot.create{selector:
   /volumes/<pv-name> (pool) or / (dedicated), hold: "csi:<VolumeSnapshotContent
   uid>"}`; a volume created `FromSnapshot` or `FromVolume` (PVC clone) =
   `clone.create{selector: <source subtree>, destination: /volumes/<new-pv-name>}`
   — always resolved against the *source volume's own pool and shard*, never
   an arbitrary target. **A cross-pool (or cross-shard) clone/restore is
   refused by default**, `INVALID_ARGUMENT` with a message naming the
   source and requested destination pools, because Constellation's clones
   are subtree-COW *within one filesystem* (§2.3) — routing a clone
   request across pools would silently fall back to a full data copy with
   none of the guarantees (instant, metadata-only) a Kubernetes user
   reasonably expects from "clone this PVC." A future explicit full-copy
   mode across pools is a documented, not-yet-built option, never the
   default. See §"CSI RPC mapping" for the full per-RPC treatment.
9. **Credentials arrive per-request, never baked into the engine-pod image
   or a long-lived Kubernetes `Secret` mount the engine pod reads for
   itself.** `StorageClass`/`VolumeSnapshotClass` reference a k8s `Secret`
   by the standard `csi.storage.k8s.io/*-secret-name`/`-namespace`
   parameters (one set of credentials per pool, since a pool is exactly the
   unit a `StorageClass` already scopes — §2.3); the CSI sidecars resolve it
   and hand it to `constellation-csi` as request bytes; `constellation-csi`
   forwards it to the engine pod as `fs.unlock`/`view.mount` params, held
   only in that engine pod's `EphemeralSecretStore` (memory-only, VERIFIED
   named in the design brief's required plan-31 seam list).
10. **The node plugin is the only privileged component.** It alone calls
    `fuse_mount_fd`/`mount(2)`/bind-mount; engine pods run unprivileged,
    holding only an inherited fd and a hostPath control socket — see
    §"Credentials and security" for the exact capability set on each pod.
11. **`fsGroupPolicy: File`.** Constellation already tracks POSIX uid/gid
    per entry and enforces them in the engine (not the kernel), so per-file
    `fsGroup` chown on mount is meaningful and cheap to apply as part of
    `view.mount`'s subtree walk on first publish — see
    §"Semantic notes" for why `None` (the safer default for network
    filesystems with no local chown story) was considered and rejected here.
12. **`podInfoOnMount: true`, `requiresRepublish: true`.** (Revised from an
    earlier draft's `false` — see §2.2's Mountpoint-drain-baseline finding.)
    The driver wants `csi.storage.k8s.io/pod.name`/`.namespace`/`.uid`/
    `.serviceAccount.name` in `NodePublishVolume`'s `volume_context` for the
    `view.mount` labels (§"Observability"), and it *also* needs kubelet to
    periodically re-call `NodePublishVolume` so that an engine-pod **crash**
    (OOM, panic — as opposed to a graceful, node-plugin-orchestrated
    `node.handoff`) is recoverable without a human: on each republish, the
    node plugin checks whether the staging mount's FUSE connection is still
    alive; if it is, republish is a no-op; if the engine pod died and took
    the fd with it (`ENOTCONN`), republish is the node plugin's cue to
    restage against a freshly created engine pod and re-bind-mount. Session
    handover (§8) makes a *planned* engine-pod replacement invisible to
    writers and does not depend on this republish cycle at all — but a
    crash is not a planned replacement, has no fd to hand off (the crashed
    process was the fd's sole owner, per §2.2's fd-ownership discipline),
    and needs exactly the always-on republish-driven remount baseline every
    other FUSE CSI driver in this space already relies on (§2.2). Static
    `false` would leave a crashed engine pod's PVs stuck `ENOTCONN` forever
    with no recovery signal.
    *Limitation (37-k3a, measured on kind):* the restage repairs the
    staging mount and the bind *at the target path*. A container that was
    already running when the engine crashed keeps its own mount of the
    target. That mount was propagated into the container's mount namespace
    when it started, so it stays a dead `ENOTCONN` mount until that
    container restarts. A new mount at the target reaches only containers
    started after it. So "recoverable without a human" holds for the volume
    and for new pods, not for running ones: their liveness probes or
    restart policy must do it. K5's handover removes the limitation for
    planned replacements, because the connection never dies there. A crash
    still has it.
13. **`seLinuxMount: false` at K0–K6, revisited at K7.** SELinux
    per-volume mount context (`-o context=`) is a FUSE mount option
    (`fuse_mount_fd`'s `opts`) applied once at `NodeStageVolume` time for
    the whole PV; since every pod publishing the same PV shares one
    underlying FUSE mount (settled decision 5), Kubernetes' "different SELinux
    label per pod on the same volume" story (the reason `seLinuxMount`
    exists) does not apply cleanly to a shared-staging design — declaring
    `false` is honest about that, and K7 revisits whether a
    `mountOptions`-level, StorageClass-wide SELinux context (rather than
    per-pod) covers the common case.
14. **Volume access modes: RWO, `SINGLE_NODE_MULTI_WRITER` (RWOP — multiple
    pods on the same node, one at a time is not required; see note below),
    ROX and RWX all advertised.** RWX is the headline feature. VERIFIED
    (`container-storage-interface/spec`, `csi.proto`, current tag `v1.13.0`)
    the access-mode enum is `SINGLE_NODE_WRITER`,
    `SINGLE_NODE_SINGLE_WRITER`, `SINGLE_NODE_MULTI_WRITER`,
    `SINGLE_NODE_READER_ONLY`, `MULTI_NODE_READER_ONLY`,
    `MULTI_NODE_SINGLE_WRITER`, `MULTI_NODE_MULTI_WRITER`. Kubernetes'
    `ReadWriteOncePod` (RWOP, GA since 1.29) maps to `SINGLE_NODE_SINGLE_WRITER`;
    plain `ReadWriteOnce` maps to `SINGLE_NODE_MULTI_WRITER` when the driver
    advertises it (multiple pods, same node, concurrent) or falls back to
    `SINGLE_NODE_WRITER` otherwise. Constellation's engine already
    serializes per-inode ops (plan 31 C4's ordering barrier) regardless of
    how many local FUSE opens reach it, so advertising
    `SINGLE_NODE_MULTI_WRITER` costs nothing extra — the engine's own
    correctness story already covers it.
15. **RWX consistency = Constellation's own `cto` (close-to-open) semantics,
    not full cache coherence.** See §"Semantic notes" — this is stated
    up front because it is the one place Kubernetes users most often assume
    NFS-like or stronger semantics without being told otherwise.
16. **The isolation boundary, and the trust model, is the `StorageClass`
    (one pool).** A pool shares one E2E key, one metadata commit chain and
    one GC domain (§2.3); within a pool, a subtree `View` confines each
    PV's mount, but the *engine pod itself* can reach the whole pool
    filesystem — it is not sandboxed per-PV. Tenants that need one PV's
    engine pod to be unable to even theoretically reach another tenant's
    data must use different `StorageClass`es (different pools, different
    credentials, different engine pods), not rely on within-pool subtree
    confinement as a security boundary. This is stated as an explicit,
    settled trust model rather than left implicit, because it is the one
    fact about pooling most likely to surprise a security-conscious
    operator who assumes "PV" already means "isolation boundary." Plan 31's
    subtree-confinement `View` guarantee (no `..`-above-root, no
    cross-subtree symlink/hardlink escape) still holds and is what makes a
    *buggy* client's own I/O safe — it is not what makes the pool's trust
    model per-tenant.
17. **Static provisioning names any existing path inside a pool.** A `PV`
    whose `volumeHandle` is `<pool-fs-uuid>/<any-existing-path>` (not
    necessarily under `/volumes/`, e.g. `<uuid>/datasets/imagenet` for a
    pre-seeded dataset a human copied in out-of-band) is mountable via the
    normal `NodeStageVolume`/`NodePublishVolume` path with no `CreateVolume`
    call at all (the standard CSI static-provisioning shape — no
    dynamic-provisioning annotation, no `storageClassName` required to
    match a real class). The read-only access mode (`ROX`) is fully
    supported for this case. `DeleteVolume` is never called for a
    statically-provisioned PV (`reclaimPolicy: Retain` is the only sane
    choice for one) and this driver does not special-case that — it is
    ordinary CSI/Kubernetes behavior.
18. **Humans may mount a pool directly, outside Kubernetes, with the
    ordinary CLI.** `constellation mount <pool-name>:/volumes/pvc-… <dir>`
    (or any other subtree) works exactly as it does for any other
    Constellation filesystem — this is a deliberate feature (inspection,
    seeding a static-provisioning dataset, exporting a PV's contents for a
    one-off job), not an oversight to close off. Safety rules, documented
    in the Helm chart's README and the how-to guide (K7): quotas set via
    `quota.set` still apply to a human's writes under `/volumes/<pv>/`
    exactly as they would to the CSI-mounted pod's; a human **deleting**
    content directly under `/volumes/<pv>/` (rather than through
    `DeleteVolume`) is not prevented (Constellation has no in-band way to
    tell "an admin's `rm -rf` under a subtree" from "an application's own
    delete"), but is *detected*: the periodic `view.list`/health-check
    reconciliation the node plugin already runs (§"Engine-pod lifecycle")
    notices a volume directory whose xattrs are gone or whose tree no
    longer matches what `view.mount` last observed, and surfaces it as an
    abnormal `VolumeCondition` (§"Semantic notes") rather than silently
    serving a now-empty mount as if nothing happened.
19. **`DeleteVolume` is a trash rename; purge is asynchronous, and it is a
    controller-side worker, not a Constellation `singleton`-elected task.**
    `DeleteVolume` renames `/volumes/<pv>/` to `/.trash/<pv>-<deleted-ts>/`
    (one metadata op — the quota is released immediately, and `DeleteVolume`
    itself returns as soon as the rename lands, matching the CSI spec's
    expectation that delete completes promptly). A background worker inside
    the leader-elected controller `Deployment` (settled decision 3's
    existing `external-provisioner`/`external-resizer`/`external-snapshotter`
    lease, which `constellation-csi` itself also holds while leading, per
    §"Deletion and purge") walks each known pool's `/.trash/` periodically
    and deletes trashed subtrees for real, rate-limited so one huge deleted
    volume cannot starve other pools' purges. **Why controller-side, not
    Constellation's own `singleton`/`SingletonLease` mechanism** (the
    pattern plan 32's `_prune`/`_snapsched` use, one lease-elected leader
    per filesystem re-elected continuously across every daemon replica for
    that filesystem): (a) `/volumes/`/`/.trash/` is a CSI-layer convention,
    not a core Constellation concept — teaching the engine's own
    lease-elected maintenance jobs about it would mean `constellation-engine`
    growing CSI-specific knowledge, which settled decision 1 already rules
    out (`crates/csi` never links against the engine; the engine does not
    link against CSI concepts either, symmetrically); (b) the controller
    already has a proven, continuous leader-election mechanism for exactly
    this class of "exactly one of N replicas does the work" job (settled
    decision 3) — introducing Constellation's `SingletonLease` as a *second*
    leadership primitive for the same shape of problem is duplication, not
    a genuine need; (c) engine pods are ephemeral per (pool shard, node)
    and subject to idle-GC (§"Engine-pod lifecycle") — a purge task
    living inside one would stop running the moment the last PV on that
    node unmounts, exactly when a pool with no currently-mounted PVs still
    needs its trash purged; the controller is the one component in this
    design meant to run continuously regardless of mount activity. Purge
    itself is just ordinary control-protocol calls (list `/.trash/`, delete
    each stale entry) against whichever engine pod is reachable for that
    pool — nothing about the operation needs to run *inside* the engine
    process the way `_prune`'s direct meta-store manipulation does. See
    §"Deletion and purge" for the full flow, including how the controller
    reaches an engine pod for a pool with no node-side mounts at all.

## 4. Architecture

```
                    ┌─────────────────────────────────────────────┐
                    │  kube-apiserver / etcd                       │
                    │  StorageClass, VolumeSnapshotClass,          │
                    │  PV, PVC, VolumeSnapshot(Content)            │
                    └───────────────┬───────────────────────────────┘
                                     │ watches/updates
        ┌────────────────────────────────────────────────────────────┐
        │ Controller Deployment (replicas: 2, leader-elected)          │
        │ ┌──────────────────┐ ┌──────────────────┐ ┌───────────────┐ │
        │ │external-provisioner│ │external-resizer │ │external-      │ │
        │ │                   │ │                  │ │snapshotter +  │ │
        │ │                   │ │                  │ │snapshot-      │ │
        │ │                   │ │                  │ │controller CRDs│ │
        │ └─────────┬─────────┘ └────────┬─────────┘ └──────┬────────┘ │
        │           └───────────┬────────┴──────────────────┘          │
        │                unix socket (CSI Controller service)          │
        │                       ▼                                      │
        │           ┌────────────────────────┐                         │
        │           │  constellation-csi       │  (crate crates/csi)    │
        │           │  Identity + Controller    │                        │
        │           └────────────┬──────────────┘                       │
        └────────────────────────┼──────────────────────────────────────┘
                                  │ constellation-control (UnixSocket,
                                  │ per-node hostPath, one per engine pod)
                                  │ fs.create, quota.*, snapshot.*, clone.*,
                                  │ view.list (trash walk, for purge)
                                  ▼
   ┌──────────────────────────────────────────────────────────────────────┐
   │ Node N — DaemonSet pod (privileged)          Engine pods (unprivileged)│
   │ ┌────────────────────────┐                  ┌─────────────────────┐  │
   │ │ node-driver-registrar   │                  │ constellation-engine-│  │
   │ │ livenessprobe           │                  │ <fs>-<node-N>        │  │
   │ │ ┌─────────────────────┐ │  hostPath unix    │ (EngineHost, N=1)   │  │
   │ │ │ constellation-csi     │◄┼── socket + ──────┤ Engine::open_view   │  │
   │ │ │ Node service          │ │  SCM_RIGHTS fd    │ per PV = View      │  │
   │ │ │ - NodeStageVolume:    │ │  (Transport::     │ view.mount         │  │
   │ │ │   fuse_mount_fd() +   │─┼─  send_fd)        │  {PreopenedFd}     │  │
   │ │ │   view.mount over     │ │                   │ FuseSession        │  │
   │ │ │   control socket      │ │                   │ (holds /dev/fuse fd)│  │
   │ │ │ - NodePublishVolume:  │ │                   └──────────┬──────────┘  │
   │ │ │   bind mount only     │ │                              │             │
   │ │ └───────────┬───────────┘ │                              │ kernel FUSE │
   │ └─────────────┼─────────────┘                              │ session     │
   │                │ globalmount staging path (kernel fd stays │             │
   │                │  in this pod's mount namespace, propagated│             │
   │                │  Bidirectional)                            ▼             │
   │                └──────────────► bind-mounted into every workload pod's   │
   │                                  volume dir (target_path)                │
   └──────────────────────────────────────────────────────────────────────────┘
```

Components:

- **`constellation-csi` binary**, two entry points selected by CLI flag or
  argv[0] symlink (matching the common CSI pattern, e.g.
  `--controller`/`--node`), both linking one shared crate `crates/csi`:
  - **Identity service**: `GetPluginInfo` (name + driver version),
    `GetPluginCapabilities` (`CONTROLLER_SERVICE`, `VOLUME_ACCESSIBILITY_CONSTRAINTS`
    *not* set — the driver has no node-affinity requirement for volumes,
    every node can reach S3), `Probe` (checks the control-protocol connection
    to at least one reachable engine pod, or trivially OK on the controller
    side where no engine pod is expected to be up yet).
  - **Controller service**: implements `CreateVolume`, `DeleteVolume`,
    `ControllerGetCapabilities`, `ControllerExpandVolume`, `CreateSnapshot`,
    `DeleteSnapshot`, `ListSnapshots`, `ValidateVolumeCapabilities`, plus the
    background **purge worker** (settled decision 19, §"Deletion and
    purge"). Talks to *any* reachable engine pod for the target pool —
    controller RPCs are node-independent registry/quota/snapshot/purge
    operations, they don't need a specific node's engine pod. Since a
    controller RPC (including purge) can arrive for a pool with no PV
    mounted on any node yet, the controller itself ensures exactly one
    lightweight **controller-owned engine pod** per (pool, shard) exists
    whenever that pool has any volume at all — see §"Engine-pod lifecycle"
    for its exact shape; it is the same `constellation-engine-<pool>-<node>`
    binary/image, just not `nodeName`-pinned (named
    `constellation-engine-<pool>-controller` instead) and never GC'd by the
    node plugin's idle-mount TTL.
  - **Node service**: implements `NodeGetCapabilities`, `NodeGetInfo`,
    `NodeStageVolume`, `NodeUnstageVolume`, `NodePublishVolume`,
    `NodeUnpublishVolume`, `NodeGetVolumeStats`, `NodeExpandVolume` (no-op,
    settled decision 6). Owns the privileged `fuse_mount_fd` call and the
    engine-pod lifecycle for its own node (§"Engine-pod lifecycle").
- **Engine pods.** For `layout: pool`, one per (pool filesystem [or
  pool-shard], k8s node) actually in use, named
  `constellation-engine-<pool>[-shard-<k>]-<node>`, hosting every `View`
  (= PV subtree) of that pool mounted on that node — sharing one `Engine`'s
  meta store, cache, lease/P2P identity and `ResourceBudget` across every PV
  of the pool on that node, which is the whole point of pooling (§2.3). For
  `layout: dedicated`, one per (`StorageClass`, k8s node), hosting N
  `Engine`s via plan 31's `EngineHost` (one per dedicated PV mounted on that
  node, sharing only the pod's runtime/cache *budget*, not a filesystem).
  Either way it's the `constellation` binary in daemon mode. Unprivileged:
  no `mount(2)`, no `CAP_SYS_ADMIN`; it only ever receives already-open fds
  over its control socket and serves them with `MountSource::PreopenedFd`.
- **hostPath layout** (per node, see §"Engine-pod lifecycle" for the full
  tree) carries the fd-passing unix sockets and engine-pod control sockets;
  it is how the privileged node plugin and the unprivileged engine pods
  rendezvous without a network hop.

Control-protocol methods this plan drives. Most are from the plan-31 §9.2
table (cited by name, not renumbered here); the `browse.*` family is a
**new addition this plan needs from plan 31**, beyond the fixed-name list
in this plan's own header — the pool design (§2.3) needs ordinary
directory-level manipulation (create a subdirectory, get/set xattrs,
rename, recursive delete, list children) scoped to the engine's own
filesystem, distinct from `View`-level FUSE serving, and no such seam was
in plan 31's original scope because pooling postdates it. Recorded here for
plan 31's session to pick up, the same way §"What plans 32 and 33 provide
for this plan" records this plan's needs from those plans:

| Method | Called by | When |
|---|---|---|
| `fs.create{bucket, prefix, params}` | controller | `CreateVolume` on first use of a pool or a dedicated PV — idempotent on `bucket`+`prefix` (settled decision 19's brief; matching params → existing fs uuid, conflicting params → error). Unconditional: there is no `autoCreateFilesystem` opt-out — "user supplies only a bucket" (§1) means the driver always owns creation. |
| `browse.mkdir`/`browse.xattr{set}`/`browse.xattr{get}` | controller | `CreateVolume`: `mkdir /volumes/<pv>` (pool) then set the `user.constellation.csi.*` xattrs (settled decision 7); idempotency checks read them back |
| `browse.rename` | controller | `DeleteVolume`: `/volumes/<pv>` → `/.trash/<pv>-<ts>` (settled decision 19) |
| `browse.readdir`/`browse.delete{recursive}` | controller (purge worker) | §"Deletion and purge": list `/.trash/`, delete each stale entry |
| `fs.unlock` | node plugin → engine pod | `NodeStageVolume`, supplying S3 + E2E credentials before the first `view.mount` on a freshly (re)started engine pod |
| `view.mount` | node plugin → engine pod | `NodeStageVolume`, with `source: PreopenedFd`, subtree `/volumes/<pv-name>` (pool) or `/` (dedicated), `labels: {pv, pvc, namespace, pool, shard}` |
| `view.unmount` | node plugin → engine pod | `NodeUnstageVolume` |
| `view.list{labels}` | controller (health/GC), node plugin (idempotency checks) | periodic reconciliation, `NodeStageVolume` retries |
| `view.stats` | node plugin | `NodeGetVolumeStats` |
| `quota.set` | controller | `CreateVolume` (initial size), `ControllerExpandVolume` |
| `quota.get` | controller | `NodeGetVolumeStats` capacity fields, `ValidateVolumeCapabilities` |
| `snapshot.create{hold}` | controller | `CreateSnapshot` |
| `snapshot.delete` | controller | `DeleteSnapshot` (releases the hold first) |
| `clone.create` | controller | `CreateVolume` with a `VolumeContentSource`, same pool/shard only (settled decision 8) |
| `node.leave` | node plugin (preStop hook, or triggered by the controller on node drain) | node/engine-pod graceful removal |
| `node.handoff` | node plugin, orchestrating an engine-pod replacement | §"FUSE session handover protocol" |

`Transport::send_fd` is the `UnixSocket` transport feature carrying the
`/dev/fuse` fd via `SCM_RIGHTS`; it is what makes `view.mount{source:
PreopenedFd}` possible across the node-plugin/engine-pod process boundary.

## 5. CSI RPC-by-RPC mapping

| RPC | Constellation operation | Idempotency key | Error mapping |
|---|---|---|---|
| `Identity.GetPluginInfo` | static | n/a | n/a |
| `Identity.GetPluginCapabilities` | static (`CONTROLLER_SERVICE`; no `VOLUME_ACCESSIBILITY_CONSTRAINTS`) | n/a | n/a |
| `Identity.Probe` | control-protocol `node.ping` against a reachable engine pod (controller), or local health (node) | n/a | any failure → `NOT_READY`-shaped `false` in `ProbeResponse.ready`, never an RPC error |
| `Controller.CreateVolume` | **Pool:** `fs.create{bucket, prefix[, shard-<k>]}` (idempotent, always run — no auto-create opt-out) to get the pool fs uuid, then `browse.mkdir{/volumes/<name>}` + `browse.xattr{set}{pv,pvc,namespace,capacity,source,created}` + `quota.set{subtree:/volumes/<name>, bytes: req.capacity_range.required_bytes}`. **Dedicated:** `fs.create{bucket, prefix/<name>}` then `quota.set{subtree:/, bytes:...}` on the new filesystem's root. | `req.name` (`pvc-<uid>`, provisioner-supplied, stable across retries) | An existing `/volumes/<name>` (or dedicated fs) with **matching** xattrs/params → `OK`; **mismatched** xattrs (different `capacity`/`source`/owner for the same name — settled decision 7) → `ALREADY_EXISTS`; quota exceeding the pool's own hard cap → `RESOURCE_EXHAUSTED`; bad StorageClass parameters (unknown `layout`, missing `bucket`) → `INVALID_ARGUMENT`; concurrent create for the same name already in flight → `ABORTED` (in-process per-`req.name` mutex, §"Concurrency guard" below) |
| `Controller.DeleteVolume` | **Pool:** `quota.set{subtree:/volumes/<name>, bytes: 0}` then `browse.rename{/volumes/<name> → /.trash/<name>-<ts>}` — a single metadata op, the actual purge is async (settled decision 19, §"Deletion and purge"). **Dedicated:** drop the whole filesystem (a batch-delete job, no trash — settled decision 7). | `req.volume_id` | already gone (no such subtree/fs, or already in `/.trash/`) → `OK` (CSI requires delete to be idempotent-safe on a missing volume, VERIFIED spec.md `DeleteVolume`: "This operation MUST be idempotent... MUST return `OK`... if the volume does not exist"); in-flight `snapshot.create` referencing it → `FAILED_PRECONDITION` |
| `Controller.ControllerExpandVolume` | `quota.set{subtree:/volumes/<name> (pool) or / (dedicated), bytes: req.capacity_range.required_bytes}` | `req.volume_id` (quota.set is naturally idempotent — setting the same value twice is a no-op) | new size below `used_bytes` → `OUT_OF_RANGE` (VERIFIED spec.md: `ControllerExpandVolume` "grow only", shrink unsupported) |
| `Controller.CreateSnapshot` | `snapshot.create{hold: "csi:<content-uid>", selector: /volumes/<source-name> (pool) or / (dedicated)}` | `req.name` | source volume busy with an incompatible op → `ABORTED`; a snapshot already exists under a *different* hold for the same `req.name` → `ALREADY_EXISTS` |
| `Controller.DeleteSnapshot` | release the `csi:<content-uid>` hold, then `snapshot.delete` if no other hold remains | `req.snapshot_id` | missing → `OK` (same idempotency rule as `DeleteVolume`) |
| `Controller.ListSnapshots` | `snapshot.list` filtered to holds prefixed `csi:` | n/a (read-only) | pagination token mismatch → `ABORTED` per spec's `starting_token` contract |
| `Controller.ValidateVolumeCapabilities` | `quota.get` (existence check) plus a static compatibility check against settled decision 14's advertised modes | `req.volume_id` | volume missing → `NOT_FOUND` |
| `Controller.ControllerGetCapabilities` | static | n/a | n/a |
| `Controller.CreateVolume` (`req.volume_content_source` set — clone/restore) | parse both `volume_id`s' pool-fs-uuid; **same pool and shard** → `clone.create{selector: source subtree, destination: /volumes/<new-name>}` (metadata-only COW, §2.3); **different pool or shard** → refused by default (settled decision 8) | `req.name` | cross-pool or cross-shard source → `INVALID_ARGUMENT` naming both pools/shards; source volume/snapshot missing → `NOT_FOUND` |
| `Node.NodeStageVolume` | parse `req.volume_id` for the pool-fs-uuid (and shard); ensure the (pool[/shard], node) engine pod exists and is `Ready` (§"Engine-pod lifecycle"); `fs.unlock` if this is the pod's first volume; `fuse_mount_fd(globalmount_path, opts)`; `Transport::send_fd` the resulting `OwnedFd` to the engine pod; `view.mount{source: PreopenedFd, subtree: /volumes/<pv-name> (pool) or / (dedicated), labels}` | `req.volume_id` + `req.staging_target_path` (re-staging the same path when already staged is a no-op, VERIFIED spec.md `NodeStageVolume` idempotency clause) | engine pod unreachable after the timeout → `DEADLINE_EXCEEDED`; wrong fs credentials → `PERMISSION_DENIED`; already staged at a *different* path → `ALREADY_EXISTS`; `volume_id` doesn't parse to a known pool-fs-uuid → `NOT_FOUND` |
| `Node.NodeUnstageVolume` | `view.unmount`; if this was the engine pod's last `View`, mark it idle (GC'd after a grace period, §"Engine-pod lifecycle") | `req.volume_id` | not staged → `OK` (idempotent) |
| `Node.NodePublishVolume` | on every call (not just first publish — `requiresRepublish: true`, settled decision 12): if the staging mount is alive, bind mount (`MS_BIND`, plus `MS_RDONLY` for ROX) from the staging path to `req.target_path` (a no-op if already bound); if the staging mount is `ENOTCONN` (engine pod crashed, §2.2), restage first (fresh engine pod, `fuse_mount_fd`, `view.mount`) then bind mount | `req.volume_id` + `req.target_path` | staging path missing (staged elsewhere/never, and restage also fails) → `FAILED_PRECONDITION`; already published and healthy at this exact path → `OK` |
| `Node.NodeUnpublishVolume` | unmount the bind mount at `req.target_path` | `req.volume_id` + `req.target_path` | not mounted there → `OK` |
| `Node.NodeGetVolumeStats` | `view.stats` (statfs-shaped: `rsize`/`rcount`, plus `quota.get` for capacity); also checks the pool subtree's xattrs are still present and consistent with what `view.mount` last recorded | n/a (read-only) | volume path not a mount → `NOT_FOUND`; subtree deleted/mismatched xattrs behind the driver's back (settled decision 18) → `OK` with `volume_condition: {abnormal: true, message: "..."}` set (§"Semantic notes"), never an RPC error — `NodeGetVolumeStats` reports condition, it does not enforce it |
| `Node.NodeExpandVolume` | no-op, returns success (settled decision 6) | n/a | n/a |
| `Node.NodeGetCapabilities` | static (`STAGE_UNSTAGE_VOLUME`, `GET_VOLUME_STATS`; not `EXPAND_VOLUME`) | n/a | n/a |
| `Node.NodeGetInfo` | static (`node_id` = the k8s node name; no `accessible_topology`, `max_volumes_per_node` from an `EngineProfile`-derived cap, default unset/unlimited) | n/a | n/a |

**Concurrency guard.** `constellation-csi`'s controller binary keeps an
in-process `HashMap<String, tokio::sync::Mutex<()>>` keyed by `req.name`
(create) or `req.volume_id` (delete/expand/snapshot); a second request for
the same key that cannot acquire the lock within the gRPC deadline returns
`ABORTED` ("operation with the given Volume ID already in progress",
matching the phrasing `csi-sanity` checks for). This is the standard pattern
every CSI driver implements — the sidecars provide *some* serialization via
their own work queues, but the CSI spec places the final idempotency/
concurrency responsibility on the driver itself (VERIFIED spec.md general
"Concurrency" section: "the CO is responsible for ensuring that there is no
more than one call `in-flight` per volume at a given time... however in some
circumstances... the Plugin SHALL handle this... by returning an `ABORTED`
error").

## 6. StorageClass / VolumeSnapshotClass parameters and secrets

Every `StorageClass` this driver provisions from defines exactly one pool
or one dedicated-layout configuration (§2.3) — `filesystem: <name>` and
`autoCreateFilesystem` from earlier drafts of this plan are gone; there is
no opt-out from driver-owned creation, matching §1's "user supplies only a
bucket."

```yaml-k8s
# Example 1: a pool StorageClass (the common case) — un-sharded.
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: constellation-rwx
provisioner: csi.constellation.dev
parameters:
  # S3 bucket the pool lives in. Required.
  bucket: "constellation-csi-pool"
  # Key prefix within the bucket. Defaults to
  # "constellation-csi/<storageclass-name>" if omitted — shown explicit
  # here for clarity.
  prefix: "constellation-csi/constellation-rwx"
  # endpoint/region are optional, for non-default S3-compatible endpoints.
  # endpoint: "https://s3.us-west-2.amazonaws.com"
  # region: "us-west-2"
  # "pool" (default) or "dedicated" — see §2.3.
  layout: "pool"
  # Number of pool filesystems PVs are hashed across. Default 1 (no
  # sharding). See §2.3's "Shards" and K0's metadata-throughput guidance.
  shards: "1"
  # Filesystem-creation defaults applied by fs.create on first use.
  chunkSize: "4MiB"
  e2e: "true"
  writeMode: "strict"
  # EngineProfile preset used when constellation-csi brings up a new
  # engine pod for this pool. "server" (plan 31 C8's dense multi-PV
  # preset) is the default and the only supported value at K0-K6; a
  # future per-StorageClass override is deferred (Risks).
  engineProfile: "server"
  # csi.storage.k8s.io/*-secret-name / -namespace are the standard CSI
  # secret-reference parameters (VERIFIED external-provisioner/
  # external-resizer honor this exact key convention): resolved by the
  # sidecars, delivered to constellation-csi as request bytes, forwarded
  # to the engine pod's EphemeralSecretStore. One secret per pool, since a
  # pool is exactly the unit a StorageClass already scopes credentials to
  # (settled decision 9). Never written to a ConfigMap, never logged.
  csi.storage.k8s.io/provisioner-secret-name: "constellation-s3-creds"
  csi.storage.k8s.io/provisioner-secret-namespace: "constellation-system"
  csi.storage.k8s.io/node-stage-secret-name: "constellation-s3-creds"
  csi.storage.k8s.io/node-stage-secret-namespace: "constellation-system"
  csi.storage.k8s.io/controller-expand-secret-name: "constellation-s3-creds"
  csi.storage.k8s.io/controller-expand-secret-namespace: "constellation-system"
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
mountOptions: []
---
# Example 2: a sharded pool — many small PVs (e.g. a CI system's ephemeral
# scratch volumes), spread across 4 pool filesystems to stay under the
# per-pool metadata throughput ceiling K0 measures (§2.3).
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: constellation-ci-scratch
provisioner: csi.constellation.dev
parameters:
  bucket: "constellation-csi-pool"
  prefix: "constellation-csi/constellation-ci-scratch"
  layout: "pool"
  shards: "4"
  e2e: "false"
  engineProfile: "server"
  csi.storage.k8s.io/provisioner-secret-name: "constellation-s3-creds"
  csi.storage.k8s.io/provisioner-secret-namespace: "constellation-system"
  csi.storage.k8s.io/node-stage-secret-name: "constellation-s3-creds"
  csi.storage.k8s.io/node-stage-secret-namespace: "constellation-system"
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
---
# Example 3: dedicated layout — one tenant per StorageClass, one
# Constellation filesystem per PV, its own E2E key and credentials.
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: constellation-tenant-a-isolated
provisioner: csi.constellation.dev
parameters:
  bucket: "constellation-csi-tenant-a"
  prefix: "constellation-csi/isolated"
  layout: "dedicated"
  e2e: "true"
  engineProfile: "server"
  csi.storage.k8s.io/provisioner-secret-name: "tenant-a-s3-creds"
  csi.storage.k8s.io/provisioner-secret-namespace: "tenant-a"
  csi.storage.k8s.io/node-stage-secret-name: "tenant-a-s3-creds"
  csi.storage.k8s.io/node-stage-secret-namespace: "tenant-a"
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
---
apiVersion: v1
kind: Secret
metadata:
  name: constellation-s3-creds
  namespace: constellation-system
type: Opaque
stringData:
  # Only present for CredentialSource::Static(ephemeral). Omitted entirely
  # when the StorageClass instead sets credentialSource: aws-default-chain
  # (settled by IRSA / EKS Pod Identity on the engine pod's own service
  # account — see "Credentials and security").
  aws_access_key_id: "..."
  aws_secret_access_key: "..."
  # Constellation E2E passphrase, if the pool/dedicated filesystem is
  # E2E-encrypted.
  e2e_passphrase: "..."
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: constellation-snapshots
driver: csi.constellation.dev
deletionPolicy: Delete
parameters:
  csi.storage.k8s.io/snapshotter-secret-name: "constellation-s3-creds"
  csi.storage.k8s.io/snapshotter-secret-namespace: "constellation-system"
```

`CredentialSource::AwsDefaultChain` is selected by a `credentialSource:
aws-default-chain` StorageClass parameter (omit the `*-secret-name`
parameters entirely in that case); the engine pod's own `ServiceAccount` then
carries the IRSA/EKS Pod Identity annotation and the AWS SDK's default
credential chain resolves it with no secret ever transiting
`constellation-csi` at all — the strictly preferred production path,
documented as such in the Helm chart's README.

**Static provisioning example** (settled decision 17) needs no
`StorageClass` parameters read by `CreateVolume` at all — the pool's fs
uuid and path are already baked into the `volumeHandle`:

```yaml-k8s
apiVersion: v1
kind: PersistentVolume
metadata:
  name: imagenet-dataset
spec:
  capacity:
    storage: 500Gi
  accessModes: ["ReadOnlyMany"]
  persistentVolumeReclaimPolicy: Retain
  csi:
    driver: csi.constellation.dev
    volumeHandle: "4f9c1e2a-.../datasets/imagenet"
    readOnly: true
    nodeStageSecretRef:
      name: constellation-s3-creds
      namespace: constellation-system
```

## 7. Engine-pod lifecycle

Naming and identity below use `<pool>` for `<bucket>/<prefix>[/shard-<k>]`
under `layout: pool`, and `<sc>` (the `StorageClass` name) under `layout:
dedicated` — both are shortened to `<unit>` where the text applies to
either.

**hostPath layout, per node** (root `/var/lib/constellation-csi/`, created
by the node plugin's `DaemonSet` with a `hostPath` volume of type
`DirectoryOrCreate`):

```
/var/lib/constellation-csi/
├── node-identity/<unit>/               # Constellation node key + roster
│                                       # state for this (pool|sc, node)
│                                       # pair — survives engine-pod
│                                       # restarts, deleted only by
│                                       # node.leave's cleanup or explicit
│                                       # node removal.
├── sockets/<unit>/
│   ├── fd-handoff.sock                # node plugin ↔ engine pod, fd passing
│   └── control.sock                   # engine pod's constellation-control
│                                       # UnixSocket server (operator role
│                                       # for the node plugin's uid; see
│                                       # "Credentials and security")
└── staging/<pv-name>/globalmount/     # NodeStageVolume mountpoints (bind
                                        # source for every pod's publish,
                                        # keyed by CSI volume_id's PV name,
                                        # not by pool-internal path)
```

**Creation.** `NodeStageVolume` for the first PV of a pool (or dedicated
`StorageClass`) on a node: the node plugin checks for a `Ready`
`constellation-engine-<unit>-<node>` pod via the Kubernetes API (list by
label `constellation.dev/pool=<pool>` or `constellation.dev/sc=<sc>`, plus
`constellation.dev/node=<node>`); if absent, it creates one — a bare `Pod`
object (not a `Deployment`/`StatefulSet`: its identity is entirely
determined by (pool|sc, node) and it is never rescheduled, only replaced in
place by the node plugin itself, which already has to orchestrate
replacement for session handover, §8) with:

- `nodeName: <node>` (no scheduler round-trip — the node plugin already
  knows exactly which node it's on);
- `hostPath` mounts for `node-identity/<unit>/` and `sockets/<unit>/`
  (`Bidirectional` mount propagation is **not** needed here — only the node
  plugin's own `mount(2)` calls need propagation *out* to the host and
  kubelet's view; the engine pod never calls `mount(2)` at all, settled
  decision 10);
- `securityContext: { runAsNonRoot: true, allowPrivilegeEscalation: false,
  capabilities: { drop: ["ALL"] }, seccompProfile: { type: RuntimeDefault }
  }` — no privileges beyond opening a unix socket and receiving an
  already-open fd over it, which needs none;
- **`restartPolicy: OnFailure`, ignores `SIGTERM` while any `View` is
  mounted, `terminationGracePeriodSeconds: 600`** (§2.2's Mountpoint-drain
  baseline, adopted verbatim as this plan's always-on default, not a
  fallback): a clean exit (the engine's own signal handler, once it has
  actually completed a `node.leave`/`node.handoff{Commit}` and has zero
  `View`s left) never restarts the pod; a crash (OOM, panic — non-zero
  exit, or a `SIGKILL` after the grace period expires) does restart the
  *container*, but the new container process is a new fd owner starting
  from nothing — the kernel FUSE connection the old process held is gone
  (§2.2), which is exactly the `ENOTCONN` `requiresRepublish: true`
  (settled decision 12) exists to recover from. This means an engine-pod
  crash is client-visible as an I/O error until the next
  `NodePublishVolume` republish, bounded by kubelet's own reconciler sync
  period — never worse than Mountpoint's own crash story, and better than
  it whenever a graceful `node.handoff` (§8) is what actually happens
  instead;
- resource `requests`/`limits` from the StorageClass's `engineProfile`
  parameter, translated into the container's memory limit plus the
  `EngineProfile.memory_budget`/`cache_budget` fields passed as daemon flags
  — the container limit is set slightly above the engine's own budget so
  the engine's internal admission control (plan 31 §6.3's backpressure
  barrier) is what sheds load, not the OOM killer (deliberately stricter
  than Mountpoint's own opt-in-only resource limits, §2.2);
- an `emptyDir` (`medium: Memory`, sized from `cache_budget`) for the
  engine's local cache, so cache eviction on pod replacement is free and
  the node's real disk isn't consumed by cache that a restart discards
  anyway (durable state — the meta store, staging writes not yet
  acknowledged to S3 — lives under `node-identity/<unit>/`, on the real
  hostPath, and survives).

**Controller-owned engine pod (pool layout only).** A controller RPC
(`CreateVolume`, `CreateSnapshot`, the purge worker, §"Deletion and
purge") can arrive for a pool with no PV mounted on any node — there is no
node-side engine pod to talk to yet, and none may ever exist if every PV of
that pool happens to live on nodes that later drain. The controller
therefore ensures exactly one `constellation-engine-<pool>-controller` pod
exists per (pool, shard) with at least one volume, created and reconciled
by the controller `Deployment` itself rather than the node plugin: same
image, same unprivileged `securityContext`, same drain discipline, but
**not** `nodeName`-pinned (the scheduler picks a node) and **exempt from
the node plugin's idle-mount TTL** — it is reaped only when the pool itself
has zero volumes left (checked by the purge worker after a purge sweep
leaves `/.trash/` and `/volumes/` both empty). It never receives a
`view.mount`; it exists solely so `fs.create`/`browse.*`/`quota.*`/
`snapshot.*`/the purge worker's `browse.readdir`/`browse.delete` always have
somewhere to go. `layout: dedicated` has no equivalent — a dedicated
`StorageClass` with zero currently-mounted PVs simply has nothing for the
controller to do (no shared pool state, no trash to purge across volumes),
so no controller-owned pod is needed there.

**Readiness.** The pod's own `constellation` daemon exposes a startup/liveness
probe hitting its control socket's `node.ping`; the node plugin (or the
controller, for its own controller-owned pod) polls Kubernetes
`Pod.status.phase == Running` plus that probe before proceeding with
`fs.unlock`/`view.mount` or any `browse.*`/purge call.

**Ownership and GC.** Each node-owned engine pod carries an
`ownerReference` to the node plugin's own `DaemonSet` Pod (so it cannot
outlive the node plugin generation that created it — a stale engine pod
from a deleted node plugin gets garbage-collected by Kubernetes itself)
plus a `constellation.dev/last-view-count` annotation the node plugin
updates on every `view.mount`/`view.unmount`. A background reconcile loop
in the node plugin (polling every 30s) deletes any engine pod whose
`view.list` returns zero views and whose `last-view-count` has been zero
for longer than a `--engine-pod-idle-ttl` flag (default 10 minutes — long
enough to absorb a PVC being briefly unmounted/remounted across a pod
restart without thrashing engine pods, short enough not to waste memory on
genuinely abandoned pools/filesystems). The controller-owned pod above is
explicitly outside this loop — its lifecycle is the controller's, not the
node plugin's.

**Node identity.** Constellation's per-node roster identity (the node key,
whatever `authority`/`registry` state a live node needs to rejoin its peer
group) is keyed by (pool|sc, k8s node), not by pod — it lives on the
`node-identity/<unit>/` hostPath directory precisely so that replacing the
engine pod (upgrade, crash, OOM) does **not** look like a node leaving and
rejoining the cluster's roster; it is the same Constellation node
re-attaching, which is what makes §8's session handover meaningful (a
`node.handoff` between two engine-pod *processes* sharing one node identity,
not two different Constellation nodes).

**Drain.** The node plugin registers a `preStop` hook (and separately,
watches for `Node.Unschedulable`/`kubectl drain` via a `PodDisruptionBudget`-aware
controller loop) that, before a node is fully drained, **first waits for
workload pods using that node's engine pods to terminate** (the same
ordering Mountpoint's own drain guidance recommends, §2.2), then calls
control `node.leave{force: false}` for every engine pod on that node —
releasing leases cleanly and removing the node from the pool/filesystem's
roster instead of leaving a ghost entry that `docs/plans/v1`'s existing
roster-fairness work (commit `27941de`, "the lock queue is served in
arrival order") would otherwise have to time out. Only once `node.leave`
completes (or the 600s grace period from the engine pod's own
`terminationGracePeriodSeconds` above expires, whichever is first) is the
engine pod itself allowed to terminate. Autoscaled node churn
(cluster-autoscaler scaling a node pool down) goes through the same drain
path — this is explicitly flagged as a correctness requirement, not an
optimization: an autoscaler that kills nodes without a drain hook would
bloat the roster with dead entries on every scale-down, exactly the failure
mode the design brief calls out ("Autoscaled churn must not bloat the
roster").

## Deletion and purge

Settled decision 19 fixes *who* runs the purge worker (the controller, not
a Constellation `singleton`); this records the full flow.

**Trash.** `DeleteVolume` (pool layout) never does a synchronous, unbounded
directory walk — it renames `/volumes/<pv>/` to `/.trash/<pv>-<deleted-ts>/`
under the pool root (one metadata op) and releases the PV's quota
immediately (`quota.set{..., bytes: 0}` before the rename, so the freed
space is reflected right away even though the bytes themselves aren't
physically gone yet). `DeleteVolume` returns as soon as the rename lands.
This is deliberately the same latency profile regardless of whether the
volume held one file or ten million — the CSI spec's "delete completes
promptly" expectation is met by construction, not by racing a large delete
against a gRPC deadline.

**Purge worker.** A background task inside whichever `constellation-csi`
controller replica currently holds the leader-election lease (settled
decisions 3 and 19) runs on a fixed interval (`--purge-interval`, default 5
minutes) per known pool:

1. `browse.readdir{/.trash/}` against that pool's engine pod (the node-owned
   one if any node has a PV of this pool mounted, else the controller-owned
   one, §"Engine-pod lifecycle").
2. For each entry older than a small grace window (`--purge-grace`, default
   1 minute — just enough to avoid racing a `DeleteVolume` retry that
   re-renders the same trashed path, not a "cooling off" period a human is
   meant to use to recover a deleted PV; that is explicitly not a feature
   this driver offers, since CSI's own `DeleteVolume` contract gives no
   place to expose an undo window to Kubernetes), `browse.delete{path,
   recursive: true}`.
3. Rate-limited (`--purge-max-concurrent-deletes`, default 4 per pool) so
   one enormous trashed volume's recursive delete cannot starve every other
   pool's purge — each pool's queue is independent, and a slow purge of one
   pool never blocks another's.
4. After a successful `browse.delete`, the trash entry is gone; the
   underlying chunk objects are **not** necessarily gone yet — Constellation's
   existing GC (mark-and-sweep over every filesystem's live tree, unrelated
   to this plan) reclaims any chunk no longer referenced by any surviving
   tree, snapshot, or other trash entry on its own schedule, exactly as it
   already does for ordinary `snapshot delete`/`clone` deletion. This plan
   adds no new GC mechanism — trash purge's whole job is to make
   `/.trash/` entries disappear from the metadata namespace so `browse.readdir`
   eventually returns empty and the pool "looks deleted" to a human browsing
   it, not to reclaim S3 bytes directly.

**Failure handling.** Every step is safe to retry from scratch:
`browse.readdir`/`browse.delete` failures (engine pod unreachable, transient
S3 error) simply leave the entry in `/.trash/` for the next tick — nothing
about a half-completed recursive delete corrupts state, because a partially
deleted subtree is still a valid (smaller) subtree for the next
`browse.delete{recursive: true}` call to finish. A controller failover
mid-purge is likewise safe: the new leader's next tick re-lists `/.trash/`
and resumes: there is no purge-specific state anywhere outside the pool's
own `/.trash/` directory listing, consistent with settled decision 7's
"the controller is stateless" design.

**Cost of huge volumes.** A single PV holding millions of small files pays
a real, possibly-slow recursive metadata delete — §"Risks" records this
explicitly as a known cost, mitigated by the rate limit above (it degrades
other pools' purge latency, not their correctness) but not eliminated; a
future incremental/resumable purge (checkpointing progress within one
trashed subtree rather than treating `browse.delete` as atomic) is a
documented, not-yet-built refinement if K6's testing shows this matters in
practice.

## 8. FUSE session handover protocol

This is the mechanism that makes an engine-pod replacement (upgrade, or a
planned move off a node) invisible to writers, instead of an `ENOTCONN`
outage. It is orchestrated by the **node plugin** (which owns both the old
and new engine pod's control sockets) via the control `node.handoff` method,
and relies on the plan-31 C4 seam `FuseSession::detach()`/`resume()`.

**Preconditions.** A replacement is triggered by: a new engine-pod image
version being rolled out (Helm upgrade bumps the engine-pod `Deployment`-equivalent
pod template — since engine pods are hand-managed `Pod`s, not a
`Deployment`, the node plugin itself watches a `ConfigMap`-sourced desired
image tag and performs the rollout, one (pool|sc, node) pod at a time), or an
explicit `node.handoff` request from an operator (plan 33 UI/CLI) moving an
engine pod for maintenance.

**Steps**, run by the node plugin against the **old** engine pod's control
socket, then the **new** one, with the FUSE fd itself never leaving kernel
ownership of either process until step 4 — the kernel continues to own the
single `/dev/fuse` file description throughout; only the *process* holding a
duplicate of that fd changes:

1. **Quiesce.** The node plugin sends `node.handoff{phase: Prepare}` to the
   old engine pod. The engine's FUSE adapter stops issuing new reads from
   `/dev/fuse` (it does not close or unmount — the kernel has no way to know
   the difference between "reader momentarily busy" and "reader gone", which
   is exactly the property this protocol exploits: **the kernel queues
   in-flight and new requests against the FUSE connection while nothing is
   reading**, VERIFIED against `fuser` 0.18.0's own session model, where
   `Session::run`'s event loop is simply a blocking read-dispatch loop over
   the fd — nothing about the kernel side of a FUSE connection requires a
   *specific process* to keep reading, only that *some* process eventually
   does, before the kernel's own bounded in-flight-request queue fills and
   callers start blocking on `write(2)`/`read(2)` to the mountpoint, which is
   the "kernel queues meanwhile" property from the design brief, and the
   reason step 2's drain has a timeout).
2. **Drain.** The old engine's `View`s finish every op already accepted from
   the (now-paused) FUSE loop — the response-barrier completion pool (plan
   31 §6.3) already guarantees every accepted op completes exactly once, so
   "drain" here means "wait for the completion pool to empty for these
   views," not any new synchronization primitive. Bounded by a
   `--handoff-drain-timeout` (default 5s); ops that would block past the
   deadline (a stuck lease wait, a slow S3 fetch) are **not** aborted — they
   are left in flight, and their state is exactly what
   `HandleTableSnapshot` must capture in step 3, so a slow op does not block
   the handover, it just travels with it.
3. **Snapshot.** The old engine calls `View::export_handles() ->
   HandleTableSnapshot` for every `View` being handed off (every PV of this
   (pool|sc, node) pair — a whole-pod handoff, not per-PV, since they share one
   FUSE fd... **wait, they do not**: settled decision 5 already established
   each PV gets its *own* `fuse_mount_fd` call at `NodeStageVolume` time, so
   each `View` has its own FUSE session and its own fd. §8's handover is
   therefore performed **once per `View`**, batched into a single
   `node.handoff` control call carrying a list, so the new engine pod comes
   up with every PV resumed together rather than one-by-one with a
   partially-migrated pod in between) and `FuseSession::detach() ->
   SessionHandoff { fuse_fd, handles, view: ViewSpec, init:
   NegotiatedInit }`. `NegotiatedInit` carries exactly the fields `fuser`
   0.18.0's own `KernelConfig` negotiates at mount time (VERIFIED,
   `fuser-0.18.0/src/lib.rs:209-220`): `capabilities` (the `InitFlags`
   bitset both sides agreed on), `max_readahead`, `max_write`, `time_gran`,
   `max_background`, `congestion_threshold`, `kernel_abi` (the negotiated
   FUSE protocol `Version`). These are exactly the parameters a *second*
   `INIT` round-trip would otherwise renegotiate — carrying them across the
   handover is what makes skipping that round-trip safe rather than merely
   convenient.
4. **Transfer.** The node plugin relays each `SessionHandoff`'s `fuse_fd`
   from the old engine pod to the new one over the same fd-passing unix
   socket mechanism as the original `NodeStageVolume` handoff
   (`Transport::send_fd`) — the node plugin is already in the fd's path for
   every PV (it opened it originally), so it is the natural relay, not a
   third leg through some other channel. `handles`, `view` and `init`
   travel as ordinary control-protocol JSON/postcard payload alongside the
   fd-bearing frame. Following the same discipline VERIFIED in Mountpoint's
   own fd relay (§2.2: `pod_mounter.go`'s `defer
   pm.closeFUSEDevFD(fuseDeviceFD)`), each hop closes its own copy of the
   fd immediately after the `SCM_RIGHTS` send succeeds — the old engine
   pod's copy is closed once the node plugin has received it (during the
   Quiesce/Snapshot steps), and the node plugin's own relay copy is closed
   once the new engine pod has acknowledged receipt, so fd ownership is
   unambiguous (exactly one process holds a live copy) at every point in
   the handover, not just at the start and end.
5. **Resume.** The new engine pod calls `Engine::open_view_resumed(spec,
   handles, ...)` to rebuild its in-memory `View` with the exact same open
   handle table (so a client's already-open file descriptors, whose
   validity is tracked kernel-side by inode+generation and engine-side by
   the handle table, keep working with no `ESTALE`/`EBADF`), then
   `FuseSession::resume(SessionHandoff)`. **This is the step the design
   brief flags as needing a vendored `fuser` patch, and this session's
   research confirms exactly why**: `fuser` 0.18.0's `Session::from_fd`
   (VERIFIED, `session.rs:186-207`) — the *only* existing constructor that
   wraps an already-open fd instead of calling `mount(2)` itself — still
   unconditionally calls `self.handshake()` immediately afterward
   (`session.rs:206`), and `handshake()` (VERIFIED, `session.rs:296-370`
   and continuing to `:453`) is a blocking loop that `receive`s from the fd
   and **requires the very first message to be a FUSE `Init` operation**,
   erroring out (`io::ErrorKind::InvalidData`, "Received non-init FUSE
   operation during handshake") on anything else — and, worse, actively
   replying `EIO` to that request's own `unique` id before erroring
   (`session.rs:342-350`), which would corrupt a real in-flight client
   request rather than merely fail to resume. **The kernel does not resend
   `INIT` on an existing FUSE connection** — `INIT` is a one-time
   handshake tied to the connection's creation at the original `mount(2)`,
   not something re-triggered by a new reader attaching to the same fd — so
   calling `Session::from_fd` on a live, already-initialized connection
   is guaranteed to hit this failure mode on the first real request the
   kernel delivers after resume, not a hypothetical edge case. This is
   `FuseSession::resume`'s entire reason to exist as a *distinct* API from
   `from_fd`: a small vendored `fuser` patch (`vendor/fuser`, the same
   vendoring pattern already used for `fjall`) adds a constructor that
   skips `handshake()` entirely and seeds `Session.proto_version`/an
   equivalent internal config directly from `NegotiatedInit`, going straight
   to the `run()` event loop. K0 is where this patch is written and proven,
   not assumed — see its gate.
6. **Cutover.** Once every `View`'s `resume()` returns `Ok`, the new engine
   pod's `constellation.dev/last-view-count` annotation goes non-zero, the
   node plugin marks the old pod for deletion (a final `node.handoff{phase:
   Commit}` lets the old pod exit cleanly rather than being SIGKILLed, for
   log/metrics hygiene only — nothing correctness-relevant happens in this
   step, unlike 1-5), and normal traffic resumes. Total wall-clock target:
   under 2 seconds for the fd-passing round trip itself (excluding step 2's
   drain wait, which is data-dependent) — measured and gated in K5.

   *As built (37-k5a, see the K5 notes):* the engine moves with the
   sessions (the state dir is exclusive), so step 6 is where the old engine
   lets go — a replica sync and an exit, not a flush — and K5b gates the
   **client-visible pause**: from the drain's start (`Prepare` received)
   until the first request is served by the new pod.

**Failure handling and rollback.** Every step above is designed so that a
failure leaves the **old** pod as the source of truth until step 6's commit:

- Steps 1-3 fail (old engine pod unreachable, `export_handles` errors) →
  abort the handoff, leave the old pod running, retry later (this is a
  no-op from the client's perspective — nothing was quiesced-and-abandoned,
  because quiescing the FUSE reader does not unmount anything, per step 1's
  key property).
- Step 4 fails (new pod unreachable, socket relay error) → same as above;
  the old pod's FUSE reader is simply resumed (`node.handoff{phase: Abort}`
  tells the old engine to go back to reading `/dev/fuse` itself) rather than
  staying paused indefinitely.
- Step 5 fails for *some* `View`s but not others (partial resume — e.g. a
  corrupt `HandleTableSnapshot` for one PV, but the rest resume cleanly) →
  the successfully-resumed `View`s stay on the new pod, the failed one(s)
  fall back to `Abort` on the *old* pod for just that `View` (both pods
  briefly coexist, each owning a disjoint subset of `View`s for the same
  (pool|sc, node) — allowed, since PV≠fd sharing already means each `View` is
  independent) and the failure is surfaced as a `node.handoff` partial-result
  response the node plugin logs and alerts on, never silently swallowed.
- Step 5 fails for *every* `View`, or the vendored resume patch itself is
  unavailable/broken in a given build → the **pre-agreed fallback from the
  design brief** applies, and — unlike an earlier draft of this plan, where
  it needed its own runtime toggle — it is now simply *the baseline this
  plan already always runs* (§2.2, settled decision 12's `requiresRepublish:
  true`): the old pod stays alive and keeps serving until kubelet's next
  `NodePublishVolume` republish notices the mount is stale and the node
  plugin restages against a freshly created engine pod the *old-fashioned*
  way (full unmount/remount, accepting the `ENOTCONN` window Kubernetes
  users of every other FUSE CSI driver already live with). No special-cased
  "flip on republish for this one filesystem" logic is needed, because that
  path is never off in the first place. This is strictly worse than a
  lossless handover, but never worse than the pre-plan-37 status quo, and it
  means a K0 finding of "handover is infeasible on some kernel/fuser
  combination" degrades this plan's value, it does not block shipping it —
  the driver ships with exactly Mountpoint's own safety net at minimum.
- **Timeouts.** `node.handoff` as a whole is bounded by
  `--handoff-total-timeout` (default 30s, covering steps 1-6); exceeding it
  triggers the same `Abort` path as an explicit failure — a hung handoff
  must never leave both pods paused indefinitely, since that *would* produce
  the exact `ENOTCONN`-shaped outage this entire mechanism exists to avoid.

## 9. Credentials and security

- **`EphemeralSecretStore`** (plan 31 seam) holds every credential an engine
  pod is given, in memory only, keyed by filesystem — never written to disk,
  never logged (the audit log records only a params digest, matching plan
  33 U1's `params_digest` convention). An engine-pod restart or replacement
  means a fresh, empty store; `fs.unlock` is re-issued by the node plugin on
  every `NodeStageVolume` against a freshly-created pod, and again as part
  of §8's `node.handoff` `Prepare` phase against the *new* pod before any
  `View` resumes (a resumed `View` still needs its filesystem unlocked to
  serve reads that miss cache).
- **`CredentialSource`** selection (StorageClass parameter
  `credentialSource`, default `static-ephemeral`):
  - `Static(ephemeral)` — the `csi.storage.k8s.io/*-secret-name` path
    (§"StorageClass parameters"), the credential bytes travel
    node-plugin→engine-pod once and are held in the ephemeral store.
  - `AwsDefaultChain` — no secret at all; the engine pod's own
    `ServiceAccount` carries IRSA (`eks.amazonaws.com/role-arn` annotation)
    or uses EKS Pod Identity; the AWS SDK's default chain resolves
    credentials inside the engine pod itself, and `fs.unlock` is called
    with no explicit key material, just a flag telling the engine to use
    its host environment's default chain.
  - `Refreshing(callback)` — rotation: a `Secret` watch in the node plugin
    (informer on the specific `*-secret-name` `Secret`) re-issues
    `fs.unlock` with the new bytes on every change, with the engine
    accepting a mid-session credential swap (this is a plan 31 engine
    capability — `fs.unlock` is defined to be re-callable, not
    mount-time-only) rather than requiring a remount.
  *As built (37-k6a):* the StorageClass values are `static-ephemeral`
  (default), `refreshing` and `aws-default-chain` (`crates/csi/src/params.rs`,
  `CredentialMode`). **Nothing reaches an engine pod but `fs.unlock`**: K2's
  per-pool credentials `Secret` read through `secretKeyRef` env vars is gone
  (and with it the controller's Secret create/patch). An engine pod of a
  class that needs credentials runs `constellation serve --await-unlock`
  (`crates/cli/src/serve.rs`): its control socket is bound before the daemon
  starts and answers only `node.ping` (so the readiness probe passes) and
  `fs.unlock` naming its `--s3` URL — everything else is `Unavailable`,
  `constellation_control::proto::AWAITING_UNLOCK` — under the daemon's own
  allowlist and audit log; the credentials are checked against the bucket
  (a wrong pair is an `fs.unlock` error and the gate keeps waiting), then
  the node starts on `CredentialSource::Static(EphemeralSecretStore)` with
  the same listening socket handed over (clients wait in its backlog), and
  only then is that `fs.unlock` answered. The plugins send it over a
  connection of its own once per pod incarnation (pod uid + container id)
  and again whenever the credentials they hold differ (a rotation, in place:
  `EphemeralSecretStore::replace` bumps one generation, and the engine's S3
  credential provider re-resolves on its next request,
  `engine::backend::SourceCredentials`). `Refreshing(callback)` is the
  plugins' Secret watch (`crate::credentials::Refresher`: list + watch with
  a `metadata.name` field selector, re-listed with backoff), whose callback
  is that `fs.unlock` — the engine side is the static store rotated in
  place, not a closure in the engine. `credentialSecretName`/
  `credentialSecretNamespace` name the watched Secret, because neither
  `CreateVolume` nor `NodeStageVolume` carries the `*-secret-name` the
  sidecars resolved. The plugins keep the last credentials per engine pod in
  memory only, so a crashed engine's restage from a `NodePublishVolume`
  still unlocks its replacement; a node plugin restarted since then can for
  a `static-ephemeral` class only when the class names
  `node-publish-secret-name` (the restage passes the publish secret),
  otherwise the restage fails `UNAVAILABLE` saying so. The node plugin
  forgets a unit's credentials (and stops its watch) once no volume of it
  is staged on the node. *37-k6a review round:* a rotation is tried before
  it is used — the running engine reads `meta.json` signed with the new
  pair and refuses the `fs.unlock` (`Denied`, the old pair stays) when S3
  refuses it; the gate also checks an E2E passphrase against the keyring
  and keeps waiting on a wrong one; `SIGTERM`/`SIGINT` end a waiting gate
  with status 0 (an engine is its container's PID 1); every S3 client's
  HTTP connector cuts error bodies down to their `<Code>`
  (`store_s3::aws_auth::RedactingConnector`: S3 auth errors echo the key id
  and string to sign into object_store's error text); the engine and both
  plugins run with core dumps off. Rotation pushes from the watch carry
  `on_behalf_of: secret:<ns>/<name>` and are retried with backoff while the
  engine is starting or cannot ask S3. `harness run
  csi-credential-revocation` proves a revoked pair is refused on versitygw
  (which validates signatures). `fs.list` reports the daemon's own
  `credentials_generation` and `credentials_in_use` (the generation its S3
  clients last signed with), which is how `csi-secret-rotation` proves a
  rotation reached the S3 clients against a floci that accepts any key.
  `aws-default-chain` pods do not wait (unless E2E, for the passphrase); the
  pod-access policies admit the EKS IRSA / Pod Identity projected token, and
  `engineServiceAccount.annotations` carries IRSA's role annotation.
  *With K5a's handoff (37-k6a merge round):* a replacement engine pod
  (§8) runs `--await-unlock` like its predecessor, so it must get the
  credentials too, without a Secret or an env var. Two ways were weighed:
  the node plugin pushing `fs.unlock` to the standby before `Receive`, or
  the old engine handing them over itself. The plugin cannot be relied on:
  it holds a `static-ephemeral` class's secret in memory only, and the
  chart upgrade that rolls the engine pods restarts the plugin first — the
  very handoff that needs the secret runs in a plugin that forgot it. So the
  old engine hands them over: a new `node.handoff{Socket}` phase,
  `Credentials`, runs **before `Prepare`** (no session is stopped yet, so it
  is outside the client-visible pause and a failure rolls back trivially).
  The sender writes the credentials `fs.unlock` gave it — the very store
  its static source signs from, so a rotation since is included, as one
  generation — as one frame onto a socketpair
  (`handoff_wire::write_secret`); the plugin relays the frame unread to the
  standby's own `Credentials` (zeroized once written — the frame buffers,
  the sender's encoding buffer, and every `proto::Secret`, so
  `UnlockCredentials` too, on drop; only a JSON decoder's scratch buffer
  for an escaped string is not; nothing logs it, and
  a malformed frame's parse error is never echoed); the standby checks the
  pair against the bucket as the gate checks an `fs.unlock` (a refused pair
  fails the step), pre-opens its backend with them (an `--await-unlock`
  standby pre-opens nothing before), refuses `Receive` until it has them,
  starts its engine on them and keeps them for its own successor (an
  engine on its environment's keys keeps only an unlocked passphrase there,
  and a later passphrase-only `fs.unlock` updates it). It is
  `node.handoff` on the same sockets that already hand over the
  `/dev/fuse` descriptors, but admin is not enough for the sender's
  `Credentials`: the caller must have matched the `kind = "service"` grant
  labelled `csi-node-plugin` (`CallCtx::service`), never the owner rule —
  anybody with `pods/exec` on an engine pod runs as its uid, which is
  admin, and `fs.unlock` is otherwise write-only — and a handoff must be
  pending: a standby waits on the state dir (`handoff.standby`, written for
  the life of its wait) and nothing is committed. A standby gets
  `forbid_core_dumps` as the gate does. **Signals:** both waits install the
  same `SIGTERM`/`SIGINT` handling before any socket exists (the gate's, and
  the standby's — which had none: as its container's PID 1 it ignored
  them). A standby honours one at once only before `Seal` (exit 0, as an
  `Abort`): the plugin sends `Commit` right after `Seal` answers without
  asking whether the standby lives, so a sealed standby that exited before
  the marker appeared could leave no process holding the sessions. Once
  sealed it notes the signal and keeps waiting — past the seal's deadline
  with no marker it exits (the sender's own deadline is earlier, so it
  serves its copies again); once the marker is there it adopts, serves, and
  passes the signal to the node (`NodeRuntime::deliver_signal`, after the
  views are resumed, so §7 defers a `SIGTERM` while they are mounted). The
  handlers themselves are handed to the node (`NodeConfig::signals`)
  rather than replaced, so a signal between the wait's end and the node's
  start is not lost either. **Lock order** (node plugin): a unit's gate (K5a; shared by every
  RPC, held alone by a handoff) → the unit's bring-up lock (`NodeEngines`)
  → the unit's credential lock (`NodeService::unit_locks`, a plain mutex
  never held across an `await`). A `refreshing` class's rotation push now
  takes the unit gate shared, so a rotation arriving mid-handoff waits and
  lands on the pod that serves afterwards instead of on the retiring one
  (after the `Credentials` step it would have been lost with it). An
  engine pod still waiting for `fs.unlock` serves no view, so a rollout
  retires it rather than handing it over. Proof: csi
  `node::tests::a_restarted_plugin_hands_over_an_engine_it_holds_no_credentials_for`,
  `a_rotation_waits_for_a_running_handoff`,
  `node::handoff::tests::an_awaiting_standby_*`; cli
  `handoff_socket::tests::only_the_node_plugin_takes_credentials_for_a_pending_handoff`,
  `serve` tests `a_serving_engines_credentials_go_to_the_node_plugin_only`,
  `a_passphrase_rotation_reaches_the_handoff`; root `serve` tests
  `an_unlocked_engines_standby_gets_its_credentials_over_the_handoff`,
  `an_awaiting_standby_stops_on_a_signal_before_any_commit`,
  `a_signal_between_seal_and_commit_loses_no_session`,
  `a_signalled_sealed_standby_exits_at_its_deadline_without_a_commit`; on kind,
  `tests/csi/k5-handoff.sh` (a `static-ephemeral` class).
- **Service principal identity.** The CSI node plugin is a *service
  principal* under plan 33's `control-acl.toml` (a targeted addition to
  U1's grant `kind`s — `unix_group`/`unix_user`/`windows_sid`/`device`,
  §"Plan 33 changes" below), matched by container uid **and** the specific
  hostPath control socket path, since a bare uid match alone (e.g. uid 0)
  would also match an unrelated root process on the host:

  ```toml
  [[grant]]
  kind = "service"
  uid = 0
  socket_glob = "/var/lib/constellation-csi/sockets/*/control.sock"
  role = "operator"
  label = "constellation-csi node plugin"
  ```

  *(37-k3a correction: the role is `admin`, not `operator`. `view.mount` and
  `view.unmount` are `Admin`-only in the method table
  (`crates/control/src/methods.rs`), so `operator` cannot stage a volume.
  The grant is also narrower than the glob above. Each node-owned engine
  pod gets its own one-socket grant for uid 0, which the node plugin writes
  into `<hostRoot>/policy/<unit>/control-allow.toml`. That directory is
  root-owned and mounted read-only into the pod. The grant is never written
  into `sockets/<unit>/`, which the unprivileged engine owns: a privileged
  writer there could be steered through a symlink the engine planted.)*
  The original reasoning: `operator` would be sufficient for every method in
  §"Control-protocol methods" — none of them are roster/security-admin
  operations reserved to `admin` (allowlist edits, audit-log reads). Every
  engine pod's `control.sock` carries this exact grant; the controller's own
  connections (for `fs.create`, `browse.*`, `snapshot.*`, `quota.*` against
  whichever engine pod it reaches, including the purge worker and any
  controller-owned engine pod, §"Engine-pod lifecycle") use the *same*
  service-principal identity —
  `constellation-csi` the controller binary and `constellation-csi` the node
  binary are, deliberately, one authorization identity, since splitting them
  would only add configuration surface without a real isolation boundary
  (both already have to be trusted with full filesystem read/write to do
  their jobs).
  *As built (37-k6a):* the grant stays `admin` (the k3a correction above:
  `fs.unlock` is admin-only too). Every control call a CSI RPC makes carries
  the PersistentVolume it is about as `Request.on_behalf_of` (a task-local,
  `constellation_control::client::on_behalf_of`), which the engine records
  in the audit line next to the principal (`on_behalf_of`, validated to a
  short token, never read by authorization); `csi-secret-rotation` reads a
  node engine pod's `control-audit.jsonl` and asserts its `fs.unlock` and
  `view.mount` lines carry `{"kind":"service","uid":0,…,"label":"csi-node-plugin"}`
  and the PV. The controller-owned pod has its grant too (37-k6a review):
  one `kind = "service"` row for uid 65532 (the relay runs as the engine's
  uid) on its socket, label `csi-controller`, baked into the image
  (`deploy/docker/controller-engine-control-allow.toml`) and named by
  `CONSTELLATION_CONTROL_POLICY`; `csi-secret-rotation` asserts that
  principal in the controller-owned pod's audit log.
- **RBAC (Kubernetes).** Standard CSI sidecar `ClusterRole`s
  (`external-provisioner`, `external-resizer`, `external-snapshotter`
  upstream-recommended rules, unmodified) plus a `constellation-csi`-specific
  `ClusterRole` scoped to: `pods` (get/list/watch/create/delete, for engine-pod
  lifecycle, namespace-scoped to wherever the driver itself is installed —
  never cluster-wide pod creation), `persistentvolumeclaims`/`persistentvolumes`
  (get, for the `pv`/`pvc`/`namespace` labels in §"Observability"), and
  `secrets` (get, scoped by the standard CSI secret-reference convention, not
  a blanket cluster-wide secrets read).
  *As built (37-k2b, 37-k3a):* only the **controller** ServiceAccount has
  `secrets` get/create/patch, in the driver namespace. It writes the pool's
  credentials Secret that engine pods reference. The **node** ServiceAccount
  has no Secret permission. That is checked on kind with
  `kubectl auth can-i get secrets --as=<node SA>` → `no`. A ValidatingAdmissionPolicy
  holds the node ServiceAccount's pod access to engine pods on the
  requester's own node: CREATE, UPDATE and DELETE are checked against the
  token's `authentication.kubernetes.io/node-name`, and no subresource is
  allowed. Created pods are held to exactly the shape the plugin builds
  (`deploy/helm/constellation-csi/templates/exec-policy.yaml`).
  *As built (37-k6a):* the controller keeps `secrets` get only (what
  external-provisioner/-resizer read); `credentials.watchedSecrets` grants
  both plugins `get`/`list`/`watch` on exactly the named Secrets of
  `refreshing` classes (`resourceNames`; the watch's field selector is what
  RBAC authorizes against the name). Verified on kind: `can-i get
  secret/<watched>` → yes, `can-i get|list|create|patch secrets` → no for the
  node ServiceAccount.
- **PodSecurity.** The node plugin's namespace runs at PodSecurity
  `privileged` (it must — `fuse_mount_fd`, bind mounts and `Bidirectional`
  mount propagation are all denied under `restricted`/`baseline`); engine
  pods run in the **same namespace** but individually declare the
  unprivileged `securityContext` from §"Engine-pod lifecycle" — PodSecurity
  admission is namespace-scoped, not per-pod, so this plan documents the
  accepted trade-off explicitly (a namespace-wide `privileged` label
  alongside individually-hardened engine-pod specs) rather than pretending a
  `restricted` label plus an exempted node-plugin `Pod` gives real isolation
  it wouldn't.
  *Recorded in 37-k2b's review:* the controller-owned engine pod also has a
  one-shot init container that runs as **uid 0 with only `CAP_CHOWN`** (no
  privilege escalation, `RuntimeDefault` seccomp) to hand the
  kubelet-created, root-owned `DirectoryOrCreate` hostPaths to the engine's
  uid 65532 — admitted only because the namespace is `privileged`; the
  engine container itself stays non-root with no capabilities.
  *As built (37-k6a):* the init container is gone from both roles. The
  controller-owned pod's state dir and socket are `emptyDir`s (no hostPath:
  it never serves a view and is reached by exec relay), so it is
  PodSecurity-`restricted` as a whole; the node plugin makes a node-owned
  pod's `<hostRoot>/{node-identity,sockets}/<unit>` itself, owned by 65532,
  mode 0700, through `O_NOFOLLOW` descriptors under root-owned parents
  (`engine_pods::prepare_unit_dirs`), and the pod mounts them `type:
  Directory`. Both roles' containers carry `restricted`'s rules (non-root,
  `drop: [ALL]`, no escalation, read-only root, `RuntimeDefault` seccomp);
  the policies now refuse an init container, any added capability, any
  `valueFrom` env, a writable root and a hostPath kubelet would create. The
  namespace stays one `privileged` namespace: a node-owned engine pod's
  hostPaths are refused by every level below `privileged`, so a separate
  namespace would not make it `restricted`. `harness k8s-scenario
  csi-pod-security` re-creates every driver pod by server dry run in a
  `restricted` namespace: controller and controller-owned engine pod
  admitted, node-owned engine pod refused for "restricted volume types
  (hostPath)" only, node plugin refused as privileged. The split is in
  `deploy/helm/constellation-csi/README.md`.
- **The controller ServiceAccount is root-equivalent unless admission holds
  it (37-k2b).** K2 grants it `pods` create/patch/delete and `pods/exec`
  create/get in the driver namespace — the engine-pod lifecycle and the
  exec relay that is the controller's only channel to an engine pod's
  control socket (PROGRESS "Plan 37 K2"). In a PodSecurity-`privileged`
  namespace that is root on every node: exec into a privileged node-plugin
  pod, patch its image, or create a privileged pod. RBAC cannot scope a
  verb to a name or a label, so the chart ships a ValidatingAdmissionPolicy
  (`templates/exec-policy.yaml`, Kubernetes ≥ 1.30,
  `controller.podAccessPolicy`, on by default) that holds every pod
  operation of that ServiceAccount — create, update, delete, exec — to pods
  named `constellation-engine-*-controller`, every pod it creates to the
  engine-pod shape (the chart's image, no host namespaces, no privileged
  container, no privilege escalation, no added capability but the init
  container's `CHOWN`, a non-root engine container, hostPaths only under
  `<hostRoot>/{node-identity,sockets}/<unit>-controller`), and every update
  to metadata only. Verified on kind by `tests/csi/k2-smoke.sh`.
  *Tightened in 37-k3a's review:* the controller's half now mirrors the
  node's. A created pod has exactly the engine shape: one container
  running `constellation serve`, probes only `control-relay`, the init
  container only `chown`, uid 65532, no runtime class, lifecycle hook or
  `envFrom`, `automountServiceAccountToken: false`, `secretKeyRef` only to
  the pool's Secret (`<pod>-credentials`), and volumes only the unit's two
  hostPaths and the scratch emptyDir, at their own paths. Engine pods of
  both roles run as the chart's `constellation-csi-engine` ServiceAccount,
  which has no RoleBinding and mounts no token; both policies admit only
  that name. The only subresource is `exec`. A second policy,
  `constellation-csi-controller-pods-exec`, matches `pods/exec` CONNECT,
  where admission's `object` is the request's `PodExecOptions`. It pins the
  command to the relay's own (`constellation control-relay [--ping]
  --socket <sock>`) in the `engine` container with no tty. This has to be a
  separate policy: one that also matches `pods` types `object` as a Pod,
  and `command` then fails the type check. k2-smoke and k3-smoke assert
  `status.typeChecking` has no warnings and run the negatives (`sh`, a
  token, another ServiceAccount, `envFrom`, …) with a positive control. With the
  policy off (clusters < 1.30) the ServiceAccount must be treated as
  node-root. Moving engine pods to their own `restricted`-capable namespace
  would remove the init container's need for `privileged` too; that is
  K6a's to weigh.
  *Weighed in 37-k6a:* not moved — see the PodSecurity note above; the
  init container is gone without it.
- **Which pods are privileged, summarized**: node plugin — yes
  (`CAP_SYS_ADMIN` via `privileged: true`, `Bidirectional` mount
  propagation). Controller — no. Engine pods, node-owned and
  controller-owned alike — no. This 1-privileged-role
  design is the direct payoff of settled decision 10 and the reason the
  fd-passing architecture exists at all: it is strictly fewer privileged
  processes than "FUSE in the node plugin" (still one) but with a *much*
  smaller blast radius per restart, and strictly fewer than "engine mounts
  its own fd" (which would require every engine pod to be privileged).

## 10. Observability

- **View labels.** Every `view.mount` call sets `ViewSpec.labels =
  {"pv": req.volume_id, "pvc": <from volume_context, podInfoOnMount>,
  "namespace": <from volume_context>, "pool": <bucket/prefix, pool layout
  only>, "shard": <k, if shards > 1>}` (settled decision 12's reason for
  wanting `podInfoOnMount` — `pvc`/`namespace` aren't otherwise visible to
  the node plugin from the CSI request alone in every code path, only
  `pod.*` fields are guaranteed present; `pvc`/`namespace` come from
  `external-provisioner`'s own convention of also injecting
  `csi.storage.k8s.io/pvc/name` and `.../pvc/namespace` into
  `volume_context` when `--extra-create-metadata` is set on the
  provisioner sidecar — this plan's Helm chart sets that flag by default).
  `pool`/`shard` are derived from `req.volume_id`'s own uuid (settled
  decision 7), not from a separate lookup, so they're always available even
  before `podInfoOnMount` resolves the rest.
- **Metrics.** `constellation_vfs_ops_total{frontend="fuse",op,outcome}` and
  `constellation_vfs_op_seconds{frontend="fuse",op}` (plan 31 §6.10) carry
  the bounded label allowlist `{view, pv, pool, shard}` — never raw
  `pvc`/`namespace` on the high-cardinality per-op histograms, to keep
  Prometheus cardinality bounded by PV count (times shard count, still
  small), not by (PV × pod × namespace) count; `pvc`/`namespace` remain
  queryable via `view.list{labels}` for a control-plane-level join (plan
  33's UI does exactly this for its screen 3, extended to also show pool →
  shard → PV, §"What plans 32 and 33 provide for this plan"). Engine-pod-level metrics (`EngineHost`
  resource-budget gauges, fd-handoff duration histogram from §8's step 6
  measurement) are additionally labeled `{pool, shard, node}` (pool layout)
  or `{sc, node}` (dedicated). The purge worker exposes
  `constellation_csi_purge_{pending,deleted,failed}_total{pool,shard}` and
  `constellation_csi_purge_duration_seconds{pool,shard}` (§"Deletion and
  purge") so an operator can see trash backlog per pool directly.
- **Events on PVCs.** `constellation-csi` emits Kubernetes `Event`s on the
  `PersistentVolumeClaim` object (via the standard `client-go`
  `EventRecorder`, the same mechanism `external-provisioner` already uses
  for `ProvisioningSucceeded`/`ProvisioningFailed`) for: engine-pod created/
  ready/replaced (handover), quota near/at limit (a `quota.get` threshold
  check on `NodeGetVolumeStats`, mirroring plan 33's own "quota thresholds"
  notification but surfaced where a Kubernetes operator actually looks —
  `kubectl describe pvc`), and session-handover fallback engaged (§8's
  degraded-mode path) so a cluster operator sees *why* a PV briefly behaved
  like a classic FUSE CSI driver instead of silently degrading.
- **Audit.** Every mutating control call `constellation-csi` makes is
  audited exactly like any other `operator`-role client (plan 33 U1) —
  `params_digest` for the request, principal `{kind: "service", uid,
  socket}` — with the CSI `volume_id` (== the PV name) already present in
  the digested params for every volume-scoped method, satisfying the design
  brief's "audit entries carry the PV name" requirement without a special
  case in the audit log format itself.

## 11. Semantic notes

- **fsGroup.** `fsGroupPolicy: File` (settled decision 11) means kubelet
  asks the driver to apply `fsGroup`-based group ownership; Constellation
  applies it as a one-time recursive chown-equivalent walk over the
  subtree during `NodeStageVolume`'s `view.mount` (only on first stage —
  Constellation's own POSIX permission enforcement, not a mount option,
  governs every op after that, so there is no "every open re-applies
  fsGroup" cost the way a kernel-native fs with `-o fsGroupChange=Always`
  might pay). For a large pre-existing subtree being mounted for the first
  time with a *new* `fsGroup`, this walk is real, non-instant work — the
  Helm chart documents a `NodeGetVolumeStats`-derived warning threshold, and
  K7's testing checklist includes a large-tree fsGroup timing scenario.
- **SELinux.** See settled decision 13 — StorageClass-wide `mountOptions`
  context, not per-pod, at K0-K6.
- **subPath.** Fully supported with no special driver-side handling —
  `subPath` is a kubelet-side bind-mount-of-a-subdirectory operation
  against whatever `NodePublishVolume` already published; Constellation's
  POSIX semantics underneath make this exactly as correct as any other
  POSIX-backed CSI driver, no different handling needed versus, say, a
  local hostPath volume.
- **Quotas vs. capacity.** `req.capacity_range` (`CreateVolume`) and
  `NodeGetVolumeStats`'s `available`/`capacity` fields both map to
  Constellation's `quota.set{subtree:/volumes/<pv> (pool) or / (dedicated)}`/
  `quota.get` — a *soft* accounting limit the engine enforces at write time
  (`ENOSPC` once the quota is hit), not a pre-allocated block range the way
  an EBS volume's size is. Under `layout: pool`, each PV's quota is a
  per-subtree cap independent of every other PV's quota in the same
  pool — pooling shares the *filesystem* and its cache, it does not share
  or cap the *sum* of PVs' quotas against the pool's own total (there is no
  pool-level quota in this plan; a future StorageClass-wide pool quota is a
  documented, not-yet-built option). This means `GetCapacity` (the optional
  Controller RPC reporting *available raw storage* for the whole backing S3
  bucket) is meaningless for an S3-backed filesystem in the way it is for a
  fixed-size block pool — this plan does not implement it (not advertised
  in `ControllerGetCapabilities`), which is a correct, deliberate omission,
  not an oversight: S3's advertised capacity is not a number Constellation
  should be putting in front of the Kubernetes scheduler's capacity-aware
  provisioning logic (`storageCapacity` in `CSIDriver` is likewise left
  unset/`false`).
- **`VolumeCondition` (health).** `NodeGetVolumeStats` sets
  `volume_condition: {abnormal: true, message}` (VERIFIED spec.md: an
  optional field on `NodeGetVolumeStatsResponse`, distinct from the RPC's
  own success/error) when the pool subtree backing a PV no longer matches
  what `view.mount` last recorded — its xattrs are gone, or its directory
  has been removed entirely — which happens only when a human bypasses
  `DeleteVolume` and edits the pool directly (settled decision 18's
  explicitly-permitted-but-detected case). This is a health signal, not an
  enforcement mechanism: Constellation does not lock humans out of a pool
  they're allowed to mount, it surfaces that something outside Kubernetes
  changed the volume so `kubectl describe pvc`/monitoring notices before an
  application does.
- **RWX consistency = `cto`.** Constellation's close-to-open semantics
  (plan 30's `cto=strict`, plan 31 §6.3's "flush is the close-to-open
  publish fence") is exactly what RWX PVs get: a write is guaranteed visible
  to another pod's *subsequent open*, not instantaneously to a concurrent
  file descriptor already held open elsewhere (no NFSv4 delegation-style
  immediate coherence, no `O_DIRECT`-shaped bypass). This is stated plainly
  here because it is the single most likely point of surprise for a
  Kubernetes user coming from EFS (which offers a comparable, not
  stronger, guarantee — VERIFIED general knowledge of NFSv4 close-to-open
  caching, consistent with EFS's own documented consistency model) or from
  a block-storage RWO mental model (which offers much stronger single-writer
  guarantees trivially, since there's only ever one writer). The Helm
  chart's README and the `StorageClass` example both carry this sentence
  verbatim.

## 12. Testing

- **`csi-sanity`** (`kubernetes-csi/csi-test`, VERIFIED latest tag
  `v5.6.0`): the standalone `cmd/csi-sanity` binary (VERIFIED its own
  README: "this package and csi-sanity are meant to test the CSI API
  capability of a driver," distinct from the driver's own unit/e2e tests),
  run in CI against `constellation-csi`'s Controller/Node/Identity gRPC
  endpoints over a unix socket, with `--csi.mountdir`/`--csi.stagingdir`
  pointed at real (tmpfs-backed) directories on the runner and
  `--csi.secrets`/`--csi.testvolumeparameters` YAML files supplying a
  throwaway local `EngineProfile` + `object_store::memory::InMemory`-backed
  filesystem so the sanity suite never touches real S3 (K1's gate).
- **Kubernetes external storage e2e** (`kubernetes/kubernetes`'s
  `test/e2e/storage/external`, invoked via the upstream `e2e.test` binary
  with `-storage.testdriver=<testdriver.yaml>`): a `testdriver.yaml`
  declaring `SupportedFsGroupPolicy: File`, the RWX/RWO/ROX/RWOP capability
  matrix, snapshot support and `dynamicPV`/`multipods` test tags, run
  against a real kind cluster (below). This is the standard conformance
  posture every CSI driver in the `kubernetes-csi` org uses, applied here
  unmodified.
- **kind cluster + FUSE in CI.** GitHub-hosted `ubuntu-latest` runners run
  Docker in **rootful** mode by default (VERIFIED via the kind maintainers'
  own bug discussion, `kubernetes-sigs/kind#2540`: kind auto-mounts
  `/dev/fuse` into node containers only for the **rootless** docker case,
  "we also know that we do not need fuse at all in rootful operation"); a
  rootful runner's kind nodes therefore do **not** get `/dev/fuse` unless
  explicitly configured. This plan's `kind` cluster config adds it via
  `extraMounts` (REPORTED as the standard workaround pattern across
  several unrelated FUSE-on-Kubernetes projects surfaced in this session's
  research — `skypilot-org/skypilot#4108`, `meta-pytorch/monarch#4917`, both
  independently landing on "the container device cgroup rejects
  `/dev/fuse` access even with `SYS_ADMIN` unless the device is bind-mounted
  in and the pod is privileged," matching settled decision 10's own
  privileged/unprivileged split; K0 re-verifies this concretely on a real
  runner rather than trusting the pattern alone):

  ```yaml-k8s
  kind: Cluster
  apiVersion: kind.x-k8s.io/v1alpha4
  nodes:
    - role: control-plane
    - role: worker
      extraMounts:
        - hostPath: /dev/fuse
          containerPath: /dev/fuse
    - role: worker
      extraMounts:
        - hostPath: /dev/fuse
          containerPath: /dev/fuse
  ```

  Two workers (not one) so RWX-across-nodes scenarios are meaningful; the
  node plugin `DaemonSet` still additionally needs `securityContext.privileged:
  true` on the pod itself (the `extraMounts` step alone only makes the
  device visible *inside the kind node container*, it does not grant a pod
  scheduled onto that node the capability to use it — the two are
  independent layers, confirmed by the same research thread).

  **K0 corrected the first layer** (§"K0 results" row 5, measured on a real
  cluster): the `extraMounts` are *not* what makes `/dev/fuse` appear in a
  rootful host's kind node. kind runs every node container `--privileged`,
  runc populates such a container's `/dev` from the host, and the
  control-plane node — which the committed config leaves without
  `extraMounts` — had a working `/dev/fuse` and served a FUSE mount from a
  privileged pod. `kubernetes-sigs/kind#2540`'s "we do not need fuse at all
  in rootful operation" is about kind's *own* explicit mount, not about
  what a privileged node container ends up with. `tests/csi/kind-config.yaml`
  keeps the `extraMounts` for determinism and for rootless/restricted
  runtimes, not because the device would otherwise be missing. The second
  layer is VERIFIED and stronger than stated above: with the device
  bind-mounted in *and* `SYS_ADMIN` granted, an unprivileged pod still gets
  `EPERM` from `open("/dev/fuse")`; `privileged: true` is what works.
- **Constellation-specific scenarios** (new `crates/harness` scenarios,
  driven against the kind cluster rather than the harness's own
  `Client`/toxiproxy S3 setup — a genuinely new harness mode, `harness
  k8s-scenario <name>`, that shells out to `kubectl`/`helm` instead of
  spawning local processes, reusing the harness's `Model` oracle and
  `eventually()` deadline-polling idioms):
  - RWX across nodes: two pods on two different kind workers, same PVC,
    interleaved writes, verified against the `Model` oracle through `cto`
    semantics (not stronger).
  - Engine-pod upgrade with writers running: zero `ENOTCONN` observed by an
    actively-writing pod across a triggered `node.handoff` (K5's headline
    scenario).
  - CSI plugin (node/controller) restart: mounts survive (this is nearly
    free to prove once engine pods hold the fd independently of the plugin,
    but it is still an explicit scenario, not an assumption).
  - Node drain → `node.leave`: roster stays clean (no ghost entries) across
    a simulated `cluster-autoscaler`-style scale-down of a kind worker.
  - Snapshot → clone → mount: full `VolumeSnapshot`/clone-PVC round trip.
  - Quota expand: `ControllerExpandVolume` while the PV is actively mounted
    and written to, verifying no interruption.
  - Secret rotation: `Refreshing(callback)` credential swap with no remount.
  - **Many PVs in one pool, across nodes**: create N PVs (e.g. 50) against
    one pool `StorageClass`, spread across both kind workers, verify all
    are independently readable/writable, one engine pod per (pool, node)
    exists (not N), and per-PV quotas are enforced independently of each
    other (§"Semantic notes").
  - **Clone/restore within a pool, and refusal across pools**: clone a PVC
    into a new PV in the *same* pool (metadata-only, verified fast — no
    `RESIZE`-shaped data copy observed) and restore a `VolumeSnapshot` into
    a new PV in the same pool; then attempt a clone/restore whose source and
    destination `StorageClass`es name *different* pools and assert
    `INVALID_ARGUMENT` (settled decision 8).
  - **Trash purge under load**: delete several PVs of varying size (including
    one large, many-small-file volume) while other PVs in the same pool are
    actively written to; verify `/.trash/` entries disappear within a few
    purge intervals, purge never blocks unrelated I/O on the pool's other
    PVs, and GC subsequently reclaims the now-unreferenced chunks
    (§"Deletion and purge").
  - **Static provisioning**: a `PersistentVolume` naming an existing
    out-of-band path (`<pool-uuid>/datasets/...`) mounts read-only with no
    `CreateVolume` call, and (settled decision 18) a human `rm -rf` under a
    dynamically-provisioned PV's `/volumes/<pv>/` is surfaced as an abnormal
    `VolumeCondition` on the next `NodeGetVolumeStats`.
  - **Human CLI mount alongside CSI**: `constellation mount
    <pool>:/volumes/<pv> <dir>` from outside the cluster, concurrently with
    the same PV mounted by a workload pod — both see each other's writes
    under `cto` semantics, and the human's quota usage counts against the
    same `quota.get` the CSI-mounted pod would see.
  - **Shard routing**: a sharded pool (`shards: 4`) with many PVs; verify
    each PV's `volume_id` routes consistently to the same shard across
    repeated `NodeStageVolume` calls, and that a clone stays within its
    source's shard (settled decision 7/8).
  - K0 (§15) additionally measures one (unsharded) pool filesystem's
    metadata-operation throughput ceiling under this same "many PVs across
    nodes" scenario, scaled up until it degrades, to produce concrete
    sharding guidance (§2.3) rather than a guessed default.
- **Parity lane `linux-csi`.** A subset of the conformance-kit harness run
  through pods instead of local processes, in plan 31's `tests/parity.py`
  framework. Since `linux-csi` genuinely differs in shape from every other
  lane (it drives PVs through Kubernetes objects, not a `Client` mounting
  locally), it is seeded with `[[expect]]` entries from day one rather than
  starting empty like `linux-fuse-process` did — most scenarios in the
  `linux-fuse` reference set simply do not apply to a Kubernetes-mediated
  mount (e.g. anything testing raw `fusermount3` flags). This TOML is meant
  to be appended to plan 31's `tests/platform-parity.toml` (that file is
  plan 31's, not edited here — this is the addition plan 31's session
  should apply once both land):

  ```toml
  [[expect]]
  scenario = "fusermount-flags"
  lanes = ["linux-csi"]
  outcome = "skipped"
  reason = "PVs are mounted by the CSI node plugin via fuse_mount_fd, never via fusermount3; the scenario has no meaning under Kubernetes-mediated mounts."

  [[expect]]
  scenario = "local-daemon-upgrade"
  lanes = ["linux-csi"]
  outcome = "skipped"
  reason = "engine-pod upgrade is exercised by the linux-csi-specific 'k8s-engine-pod-handoff' scenario instead, which covers the same FuseSession::resume seam through node.handoff rather than 'constellation daemon --upgrade'."

  [[expect]]
  scenario = "*"
  lanes = ["linux-csi"]
  outcome = "skipped"
  cap = "cluster_locks"
  reason = "cross-node advisory locking scenarios run as k8s-scenario RWX cases with pod-granularity clients, not the harness's local multi-process Client; the underlying cluster_locks capability itself is exercised, just via a different scenario name — tracked, not silently dropped."
  ```
- **Style rule compliance**: per plan 31 §12's own rule, wildcard entries
  are capability-scoped (the third entry above carries `cap =
  "cluster_locks"`), never a bare unscoped `scenario = "*"`.

## 13. CI

No new CI job is needed for the pool scenarios added to §"Testing" — they
are ordinary `crates/harness` `k8s-scenario`s, and `kind-e2e`'s existing
`harness k8s-scenario --all` step already runs every registered scenario,
pool ones included, once they exist. The four jobs below are otherwise
unchanged by volume pooling.

```yaml
  csi-unit:
    name: crates/csi unit tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo test --workspace -p constellation-csi

  csi-sanity:
    name: csi-sanity against constellation-csi
    needs: csi-unit
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: sudo apt-get update && sudo apt-get install -y fuse3
      - run: cargo build --release -p constellation-csi
      - run: |
          go install github.com/kubernetes-csi/csi-test/v5/cmd/csi-sanity@v5.6.0
      - run: |
          target/release/constellation-csi --node --controller \
            --endpoint unix:///tmp/csi.sock --backend memory &
          sleep 2
          csi-sanity --csi.endpoint=/tmp/csi.sock \
            --csi.mountdir=/tmp/csi-mount --csi.stagingdir=/tmp/csi-stage \
            --csi.secrets=tests/csi/sanity-secrets.yaml

  kind-e2e:
    name: kind cluster CSI e2e (FUSE-enabled nodes)
    needs: csi-sanity
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo build --release -p constellation-csi -p constellation
      - uses: helm/kind-action@v1
        with:
          config: tests/csi/kind-config.yaml
      - run: bash tests/csi/install-native-s3.sh
      - run: bash tests/csi/install-driver.sh
      - run: bash tests/csi/run-e2e.sh --storage.testdriver tests/csi/testdriver.yaml
      - run: target/release/harness k8s-scenario --all --kubeconfig "$HOME/.kube/config"

  upgrade-under-load:
    name: engine-pod session handover with active writers
    needs: kind-e2e
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo build --release -p constellation-csi -p constellation -p constellation-harness
      - uses: helm/kind-action@v1
        with:
          config: tests/csi/kind-config.yaml
      - run: bash tests/csi/install-native-s3.sh
      - run: bash tests/csi/install-driver.sh
      - run: |
          target/release/harness k8s-scenario engine-pod-handoff-under-load \
            --kubeconfig "$HOME/.kube/config" --results-json handoff-results.json
      - run: python3 tests/csi/assert-zero-enotconn.py handoff-results.json
      - uses: actions/upload-artifact@v4
        if: always()
        with: { name: upgrade-under-load, path: "handoff-results.json" }
```

These four jobs join `nightly.yml` (the FUSE/kind requirements and CSI
sidecar downloads make them too slow for the PR-lane budget plan 31 §12 sets
— `csi-unit` is the one exception, cheap enough for `ci.yml`).
`upgrade-under-load` is the direct CI expression of §8's headline guarantee
and is treated as a release gate, not an informational job (§"Definition of
done").

## 14. Packaging

- **Images.** One static `musl` binary (matching plan 31's existing "one
  static binary on Linux" posture for `crates/cli`) built for
  `constellation-csi`, published as two image tags (`-controller`, `-node`,
  or a single image with the entrypoint chosen by flag — this plan defaults
  to a single image, matching the common CSI-driver convention, since it
  halves the image-build/scan surface for no real downside) plus the
  existing `constellation` engine-pod image (already built by plan 31's own
  CI for the plain daemon; this plan adds no new engine binary, only new
  daemon-mode flags — `EngineHost` with N=1, `MountSource::PreopenedFd`).
- **Helm chart** (`charts/constellation-csi/`), standard CSI chart shape:
  - `Deployment` (controller) with the three sidecars pinned to the versions
    VERIFIED current at the time of this research (`gh api
    repos/kubernetes-csi/<repo>/releases/latest`, checked 2026-09-28):
    `external-provisioner:v6.3.0`, `external-resizer:v2.3.0`,
    `external-snapshotter:v8.6.0` (plus the separately-deployed
    `snapshot-controller` at the same tag and the `VolumeSnapshotClass`/
    `VolumeSnapshotContent`/`VolumeSnapshot` CRDs it owns — installed once
    per cluster, not per driver, documented as a chart prerequisite rather
    than a subchart, matching upstream `external-snapshotter`'s own
    installation guidance of "install CRDs and the common
    snapshot-controller once, cluster-wide").
  - `DaemonSet` (node) with `node-driver-registrar:v2.18.0` and
    `livenessprobe:v2.20.0`.
  - `values.yaml` exposing: image tags/repos, `engineProfile` defaults,
    `engine-pod-idle-ttl`, `handoff-drain-timeout`/`handoff-total-timeout`,
    `purge-interval`/`purge-grace`/`purge-max-concurrent-deletes`
    (§"Deletion and purge"), resource requests/limits for every container,
    and the PodSecurity posture note from §"Credentials and security"
    spelled out in a chart-level `NOTES.txt` warning about the node-plugin
    namespace's `privileged` requirement, plus a documented note on the
    pool trust model (settled decision 16): different tenants need
    different `StorageClass`es, not just different PVCs.
  - `CSIDriver` object templated from settled decisions 2/4/11/12/13.
  - A `templates/tests/` Helm test hook running a minimal PVC
    create/mount/write/delete round trip, for `helm test` post-install
    verification independent of the full e2e suite.

## 15. Milestones

Each milestone ends with the CONVENTIONS gates green on Linux, plus this
plan's own additional gates below. K0 and K5 are the risky ones — K0
because the fuser session-resume patch is unproven until built, K5 because
"zero `ENOTCONN` under load" is a hard real-time property, not a
best-effort one.

### K0 — Spike: fd-passing mount + session handover prototype, and pool metadata-throughput ceiling (timebox: 7 days; Track B runs in parallel)

**What to build (Track A — handover).** A throwaway example outside the
product, exercising the full chain end to end without any Kubernetes
involved yet: `crates/frontend-fuse/examples/handover_probe.rs` (or an
equivalent temporary binary): mount via `fuse_mount_fd` at a scratch path,
pass the fd to a second process over a unix socket (`SCM_RIGHTS`), have the
second process wrap it and serve a trivial in-memory tree, then — the
actual spike — hand it off to a *third* process using the vendored
`FuseSession::resume` patch, with a `dd`/`fio` write loop running against
the mountpoint throughout, asserting zero I/O errors observed by the writer
across the handoff.

**What to build (Track B — pool metadata throughput, independent of Track
A and of any CSI/Kubernetes plumbing).** A `crates/harness`-driven
micro-benchmark against a real `constellation` daemon and one Constellation
filesystem: many concurrent clients doing the exact metadata-op sequence
`CreateVolume` performs (`mkdir` + `setxattr` × 6 + `quota.set`) at
increasing subtree counts/concurrency, measuring ops/sec and p99 latency
until the shared metadata commit chain (§2.3) visibly degrades. This
answers a concrete, standing question this plan's pool design otherwise
leaves as a guess: "how many PVs can one unsharded pool hold before an
operator should set `shards > 1`?" It needs no CSI code, no Kubernetes, and
no fd/FUSE machinery — it can run in parallel with Track A within the same
7-day timebox.

**Questions:**

1. Does the `fuser` `Session::from_fd` handshake failure mode predicted in
   §8's step 5 (EIO reply to a real request, then a hard error) actually
   reproduce when a second process opens an already-initialized fd and
   calls the existing `from_fd`? (Expected: yes — confirm before building
   the patch, so the patch is validated against a reproduced failure, not
   a theoretical one.)
2. Does the vendored resume patch (skip `handshake()`, seed `Session`
   directly from a carried `NegotiatedInit`) work as designed — does the
   kernel accept ordinary read/write/getattr requests on the resumed
   session with no re-`INIT`?
3. What is the actual wall-clock cost of the fd-passing round trip
   (`SCM_RIGHTS` send + receive + `Session` reconstruction), measured, not
   estimated — this is the number §8's "under 2 seconds" target is checked
   against.
4. Do in-flight requests genuinely survive the pause (step 1-2 of §8) with
   the kernel queueing them, or does the kernel's own bounded queue depth
   (VERIFIED to exist, but its exact size/behavior under a paused reader
   is a K0 measurement, not assumed) cause client-visible blocking or
   errors before the drain timeout, at realistic queue depths?
5. Does `kind` on a `ubuntu-latest` GitHub Actions runner actually mount
   `/dev/fuse` successfully into a node with the `extraMounts` config from
   §"Testing", and does a `privileged: true` pod scheduled onto that node
   successfully call `fuse_mount_fd` against it? (Confirms the §"Testing"
   REPORTED claims with a real run, per this plan's VERIFIED/REPORTED
   discipline.)
6. **(Track B)** At what subtree count/concurrency does one unsharded pool
   filesystem's metadata-op throughput start visibly degrading (latency
   knee, not just a soft slope), and does the degradation curve look like
   it is dominated by the shared commit chain (§2.3) specifically, rather
   than some unrelated bottleneck (e.g. the benchmark client itself)? The
   answer becomes the sharding guidance published in the how-to guide (K7)
   and the Helm chart's `NOTES.txt` (e.g. "shard past N PVs per pool").

**Pre-agreed consequences** (from the design brief, restated precisely):

- **Handover (questions 1-2) works as designed:** proceed to K1-K7 as
  planned; the vendored patch is upstreamed as a plan-31 C4 deliverable
  (`vendor/fuser`).
- **Handover is infeasible even after reasonable patch iteration** (e.g. the
  kernel's FUSE connection state has some other process-identity coupling
  beyond `INIT` that this session's `session.rs` reading didn't surface):
  fall back to the brief's pre-agreed degraded mode — document
  restart-with-remount, accept the `ENOTCONN` window, **or** keep the old
  engine pod alive until PVs republish via `requiresRepublish` (§8's
  "Failure handling" section already specifies this as a *runtime* fallback
  for individual failed handoffs; if K0 shows it is the *only* available
  mode, it becomes the default and only mode, and §8's protocol steps 1-3
  and 5 are removed from the plan rather than shipped unused, with §2's
  decision table's verdict on the chosen option revisited honestly in a
  follow-up note rather than silently kept).
- **`kind` + `/dev/fuse` (question 5) does not work on GitHub-hosted
  runners:** fall back to a self-hosted runner with FUSE support for the
  `kind-e2e`/`upgrade-under-load` jobs specifically (documented as a
  concrete follow-up, not an open question), keeping `csi-unit`/`csi-sanity`
  (which don't need a real kernel FUSE mount inside a nested container) on
  hosted runners.
- **More than one of the above fails:** stop and report before starting K1;
  a CSI driver that cannot mount FUSE in its own CI cannot be tested
  honestly, and a plan built on an unproven handover mechanism should not
  proceed past the spike.

**Gate:** questions 1-5 (Track A) and question 6 (Track B) answered and
recorded in §"K0 results", with the consequence taken for each explicitly
stated. Track B has no pass/fail consequence the way Track A does — it
produces a number (or a curve), not a go/no-go — but it is still gated
here, not deferred to K7, because the sharding guidance it produces is
needed before K2's `StorageClass` parameter defaults and the how-to guide
can honestly recommend a `shards` value.

### K1 — `crates/csi` skeleton, Identity service, driver registration

- `constellation-csi` binary (controller/node entrypoints), `tonic`-based
  gRPC server, Identity service fully implemented.
- `CSIDriver` object, `node-driver-registrar` wiring, Helm chart skeleton
  (Deployment/DaemonSet shapes, no sidecars beyond registrar/livenessprobe
  yet).
- **Gate:** CONVENTIONS gates; `csi-sanity`'s Identity-service test group
  passes; the driver registers successfully against a bare kind cluster
  (`kubectl get csidriver`/`csinode` show it).

### K2 — Controller service: volumes and expansion

- `CreateVolume` (pool: `fs.create` + `browse.mkdir`/`setxattr` + xattr-based
  idempotency, settled decision 7; dedicated: `fs.create` per PV),
  `DeleteVolume` (pool: `browse.rename` to trash, settled decision 19;
  dedicated: batch filesystem delete), `ControllerExpandVolume`,
  `ControllerGetCapabilities`, `ValidateVolumeCapabilities` against
  `fs.create`/`browse.*`/`quota.*`. `StorageClass` parameters
  `bucket`/`prefix`/`layout`/`shards` parsed and validated (§"StorageClass
  parameters") — `filesystem`/`autoCreateFilesystem` from earlier drafts do
  not exist in this milestone or any later one.
- `external-provisioner` + `external-resizer` sidecars wired into the Helm
  chart; a pool `StorageClass` provisions a bound `PersistentVolume` end to
  end (no mounting yet — `NodeStageVolume` still stubbed). Dedicated layout
  can land in this milestone or be deferred to K3 if `fs.create`-per-PV
  needs more plan-31 support than pool's shared-fs path — the executing
  session records which in `PROGRESS.md`.
- **Gate:** CONVENTIONS gates; `csi-sanity`'s Controller test group passes
  (against the in-memory backend, per §"Testing"); `kubectl apply` a pool
  `StorageClass`+`PVC` reaches `Bound`; a second `PVC` against the same
  pool `StorageClass` reaches `Bound` against the *same* underlying
  Constellation filesystem (verified via `fs.create`'s idempotent uuid).

### K3 — Node service: stage, publish, mount, RWX

- `NodeStageVolume`/`NodeUnstageVolume` (real `fuse_mount_fd` +
  `view.mount{PreopenedFd}` against `req.volume_id`'s parsed pool-fs-uuid
  [+shard], on-demand engine-pod creation per §"Engine-pod lifecycle",
  minus GC/drain), `NodePublishVolume`/`NodeUnpublishVolume` (bind mount,
  `requiresRepublish: true` wired per settled decision 12),
  `NodeGetVolumeStats` (including the `VolumeCondition` check, §"Semantic
  notes"). Dedicated layout, if deferred from K2, lands here.
- **37-k3a notes (deviations and limitations).**
  - *Dedicated layout:* the controller side landed. `CreateVolume` runs
    `fs.create` at `<class prefix>/<pv>` through that filesystem's own
    controller-owned engine pod, writes the record on its root and a
    filesystem-wide cap with `quota.set{/}`, and puts the prefix in the
    volume context. `ControllerExpandVolume` works. `DeleteVolume` of a
    dedicated volume answers `FAILED_PRECONDITION` until K6b's purge
    primitive exists.
  - *One engine pod per filesystem, per node:* the node side brings up one
    engine pod per **filesystem** per node. For a dedicated class that
    means one per PV per node, not §4's one per (StorageClass, node). An
    engine daemon serves exactly one filesystem (`serve --s3 <one
    location>`, and a view names no filesystem), so sharing a pod across a
    class's filesystems needs a multi-filesystem engine. That is a plan 31
    change, not a CSI one. Recorded here for K7, where it either lands or
    §4 is amended.
  - *Credentials:* the node ServiceAccount has no permission on Secrets
    (§9, 37-k3a review). A node-owned engine pod references the pool's
    credentials Secret, which only the controller writes, on
    `CreateVolume`. kubelet resolves the reference through the node
    authorizer. The node-stage secret reaches the running daemon through
    `fs.unlock`.
    - An engine opens S3 at start, so "start locked and wait for
      `fs.unlock`" would need a daemon that serves the control socket
      before it has a filesystem. That is a restructuring of
      `constellation serve` beyond this chunk.
    - A pool that only static PVs use has no such Secret. Its node pods
      run on ambient credentials (IRSA, Pod Identity) or not at all. K6a
      revisits this.
- `kind-e2e` CI job stood up (§"CI"), including the `extraMounts` +
  privileged node-plugin config validated in K0.
- **37-k3b notes.**
  - *`harness k8s-scenario`* is in `crates/harness/src/k8s*`: kind only
    (it loads the image with `kind load` and reads the workers' mount
    tables with `docker exec`). The scenarios run their file operations
    as shell scripts in the pods and compare a listing taken there with
    the `Model`. `csi-rwx-across-nodes` checks eventual convergence under
    the default `--cto bounded`. It asserts nothing about what a lagging
    reader sees: a read racing the replica's catch-up is not covered by
    close-to-open.
  - *`kind-e2e`* differs from §13's sketch. `install-native-s3.sh`,
    `install-driver.sh`, `run-e2e.sh` and `testdriver.yaml` are K7's, with
    the upstream e2e suite. Today the job runs `tests/csi/sanity-kind.sh`
    and `harness k8s-scenario --all`, which bring up their own clusters,
    S3 and chart. It runs on a self-hosted `fuse` runner (K0, question 5),
    and only when the repository variable `CONSTELLATION_FUSE_RUNNER` is
    `true`, so that a nightly with no such runner does not queue forever.
  - *Fixed in k3a's node plugin:* the `last-view-count` annotation could
    end up stale. Concurrent stages each counted and then patched, and
    their patches landed out of order: `csi-many-pvs-one-pool` saw 23 of
    25. A stale `0` with `idle-since` on a pod that is serving views is
    exactly what K6b's idle GC would collect. Counting and patching are now
    serialized, one lock per engine pod unit, and the patch is bounded by
    10 s (a timed-out or failed patch is logged and the unit's next stage
    or unstage records the count). Regression tests:
    `concurrent_stages_leave_the_last_view_count_not_a_stale_one`,
    `a_hung_view_count_patch_holds_up_neither_other_units_nor_its_own`.
- **Gate:** CONVENTIONS gates; `csi-sanity`'s Node test group passes against
  a real kind cluster (not the in-memory K1/K2 backend); a pod mounts a PV
  and reads/writes through it; the RWX-across-nodes `k8s-scenario` passes;
  the "many PVs in one pool, across nodes" `k8s-scenario` passes with
  exactly one engine pod per (pool, node) observed, not one per PV.

### K4 — Snapshots and clones

- `CreateSnapshot`/`DeleteSnapshot`/`ListSnapshots` against
  `snapshot.create{hold}`/`snapshot.delete` (depends on plan 32's
  `held_by` owner namespaces and REFER accounting — §16; K4 starts only
  after plan 32 is committed, with no ad hoc hold-naming scheme in the
  meantime).
  `CreateVolume`-from-`VolumeContentSource` (clone/restore) against
  `clone.create`, routed to the source's own pool and shard, with
  cross-pool/cross-shard requests refused `INVALID_ARGUMENT` (settled
  decision 8).
  `external-snapshotter` sidecar + `snapshot-controller`/CRDs added to the
  chart's prerequisites.
- **Gate:** CONVENTIONS gates; the snapshot→clone→mount `k8s-scenario`
  passes; the "clone/restore within a pool, and refusal across pools"
  `k8s-scenario` passes both halves; `ListSnapshots` pagination matches
  `csi-sanity`'s expectations.
- **37-k4 notes (deviations and decisions).**
  - *The hold's owner is `csi:<snapshot_id>`, not `csi:<VolumeSnapshotContent
    uid>`* (settled decision 8, §5, §16). No CSI request carries the
    content's uid: external-snapshotter sends `req.name`
    (`snapshot-<VolumeSnapshot uid>`) and at most the content's *name*. The
    snapshot id is what the content records as `status.snapshotHandle`, so
    the owner still names the Kubernetes object, and it makes every
    `csi:`-held row self-describing for `ListSnapshots`.
  - *`snapshot_id` = `<source volume_id>@<req.name>`*: the engine's own
    `path@name` selector with the volume id for the path, so the id names
    the filesystem, the shard, the subtree and the snapshot. Snapshot names
    may not contain `/`, `@`, `%`, `*` (the engine's rule for new names);
    the 128-byte name limit is CSI's.
  - *A clone lands in the source's shard*, not in `hash(req.name)`'s: the
    destination is the class's pool shard the source lives in, and that
    filesystem must be the source's (`fs.create`'s uuid compared). A
    source in another pool, in a shard the class does not have, in a
    dedicated filesystem, or a dedicated destination class, is
    `INVALID_ARGUMENT` naming both filesystems.
  - *A volume clone (`FromVolume`) goes through a transient snapshot*:
    `clone.create` clones snapshots only, so the controller takes an
    unheld `csi-clone-<hash of the new name>` snapshot of the source,
    clones it and deletes it (the same name on every retry). It is deleted
    on every path out of the create, failures included (best effort,
    logged): it is manual and unheld, so plan 32's expiry never removes
    it, and a clone abandoned with its PVC would otherwise pin the
    source's chunks forever. Only a controller crash between the snapshot
    and the delete leaves one, until that create's retry.
  - *`DeleteSnapshot` with another hold*: when a human holds the snapshot
    too (`snapshot hold --force` moved the owner to `user:…`, or a plain
    hold), the RPC succeeds and the snapshot stays the human's.
  - *`ListSnapshots` unfiltered* covers the filesystems an engine pod
    serves now (it starts none); by `snapshot_id` it also finds snapshots
    this driver does not hold (a pre-provisioned content importing a
    human's snapshot). `GET_SNAPSHOT` (alpha) is implemented and
    advertised too: `ListSnapshots` by id, `NOT_FOUND` when missing.
  - *Deletes start nothing they do not need* (the 37-k3b review's bug).
    `DeleteVolume`/`DeleteSnapshot` use an engine pod that is already up;
    with none, they ask the API server whether the PV/content still exists
    and answer `OK` at once if not, else start the pod, delete, and stop it
    again. **Known window:** external-provisioner also calls `DeleteVolume`
    with no PV when it cleans up after failing to save the PV. If then no
    engine pod is up and this process did not create the volume (the
    controller restarted in between), the answer is `OK` without trashing
    it (logged at warn with the volume id), and the volume and its quota
    stay until removed by hand. A volume this process created and has not
    deleted is exempt: the controller remembers it, starts the engine and
    trashes it (a delete repeated after that finds nothing remembered and
    starts nothing).
  - *Snapshot secrets are ignored.* §6's `VolumeSnapshotClass` example
    names `snapshotter-secret-*`, but the driver never reads
    `CreateSnapshotRequest.secrets`: the engine pod of the source's
    filesystem is already running, or is rebuilt from the PV's/the
    StorageClass's own parameters (§6), so a snapshot class needs no
    secret.
  - *Known CSI gap: unfiltered `ListSnapshots` covers only filesystems
    with an engine pod up*, so snapshots in idle pools are not listed (by
    `snapshot_id` or `source_volume_id` the filesystem is reached). Starting
    every pool's pod for a listing would be worse.
  - *`CreateSnapshot` reads every snapshot* of each running filesystem for
    the name-collision check (`snapshot.list` has no name filter): O(all
    snapshots, plan 32's automatic ones included) per create.
  - *Snapshot CRDs and the snapshot-controller* are installed by
    `tests/csi/snapshot-crds.sh` (external-snapshotter's manifests at the
    pinned tag; upstream's controller manifest at v8.6.0 still names the
    v8.5.0 image, so the script pins it). The chart enables the
    csi-snapshotter sidecar by default.
  - *A refused restore delays the namespace's deletion by minutes*:
    external-provisioner keeps `volumesnapshot-as-source-protection` on the
    source `VolumeSnapshot` while the refused PVC is in its "infeasible,
    retries delayed" backoff (204 s in one run of
    `csi-clone-cross-pool-refused`). Kubernetes behaviour, not the driver's.

### K5 — FUSE session handover in production

- *(From 37-k3a, settled decision 12's limitation.)* A container that was
  running when its engine pod crashed keeps a dead (`ENOTCONN`) mount until
  it restarts. The republish restage reaches only new mounts. K5's handover
  is what removes this for every planned replacement, and the
  `engine-pod-handoff-under-load` gate below is where that is shown.
- §"FUSE session handover protocol" implemented against real engine pods
  (not K0's throwaway probe): `node.handoff` control method, the node
  plugin's rollout orchestration for engine-pod image upgrades, full
  failure-handling/rollback per §9.
- `upgrade-under-load` CI job (§"CI") passes as a release gate, not
  informational.
- **Gate:** CONVENTIONS gates; the `engine-pod-handoff-under-load`
  `k8s-scenario` shows **zero** `ENOTCONN`/`EIO` observed by an actively
  writing client across an engine-pod replacement, across 20 consecutive
  runs (flake-proofing this specific claim, since it is this plan's
  headline guarantee and a one-in-twenty failure rate would be a real
  regression hiding behind a green checkmark); the handoff wall-clock stays
  under the §8 "under 2 seconds" target at p99 — measured as the
  client-visible pause the K5 notes define (drain start to the first
  request the new pod serves).

**K5 notes (37-k5a, part 1: the protocol, the rollout, one kind run).**
What §8 became against real engine pods, where it had to differ:

- **The state dir's lock decides who serves, so the engine moves with the
  sessions.** The replacement mounts the same `node-identity/<unit>/`
  (§7), and an engine opens its state dir exclusively (`daemon.lock`). It
  cannot resume a view before the old engine has let the state dir go, so
  §8 step 6 is not log hygiene: the old pod's `Commit` writes a commit
  marker into the state dir (`handoff.committed`), closes its views, stops
  its engine and exits 0, and the replacement — waiting as a *standby*
  (`serve --handoff-socket`, entered when the state dir is held) — takes
  the lock and resumes (`open_view_resumed`, `FuseSession::resume`). The
  lock makes double-serving impossible by construction. Everything before
  `Commit` is undone by `Abort`; after it nothing can be (the receiver
  failing to resume ends those mounts; kubelet's republish restages them,
  the §8 fallback).
- **The commit drains nothing (37-k5a fix round).** The receiver is the
  same node identity on the same `meta.db`, so the old engine stops with
  `Engine::stop_for_local_handoff` — replica synced, background loops
  stopped — and leaves the journal, the pending chunk uploads, the held set
  and the lease exactly where a restart finds them: the receiver drains the
  uploads, ships the journal, and re-adopts the unreleased lease at its
  epoch — through the takeover gate, not a plain renewal (M4's own-lease
  `Plan::Claim`; its log line reads `takeover=true marker=true`, epoch
  unchanged). What the
  stop leaves is what a process crash leaves at that instant (commits reach
  the OS on commit; a crash is already lossless and re-adopted) plus the
  sync. Nothing has to reach S3 before the receiver may open the state dir.
  The first version flushed (`shutdown_for_handover`: chunk uploads,
  journal ship, lease release — stall limit 120 s, longer while it makes
  progress) inside the commit, which a busy writer on real S3 could stretch
  past every deadline. Proof: `engine` `node::tests::a_local_handoff_stop_leaves_everything_to_the_successor`
  (write-back data whose chunks the bucket refuses, an open file whose
  handle travels; a successor on the same state dir ships it all, the lease
  keeps its epoch, an `fsync` through the handed handle succeeds, and a
  third node reads every byte from the bucket), and the root
  `serve` test `a_busy_write_back_writer_crosses_a_handoff`.
- **The backup holds its seal watch across the commit (37-k5a fix
  round).** While the receiver's engine starts, the holder is silent on
  P2P, and a backup seals the epoch after `backup_takeover_ms` (1.5 s):
  the review saw `holder silent: sealed its epoch` on every cycle, then
  the receiver without a backup (`a backup sealed our epoch`) and a
  backup brought up again ~7 s later. Two options were weighed: a mark
  the backup could see (it reads nothing of the holder's state dir, and
  an S3 mark would need a lease read before every seal, which the watch
  deliberately does not do) or the old engine telling its backup before
  it stops. The second is built: `stop_for_local_handoff` first asks
  each committed backup to hold (`PeerMsg::BackupHold` /
  `Payload::BackupHold`, one backup round trip, bounded at 2 s), and a
  backup of that holder at that epoch counts no silence for 15 s
  (`LOCAL_HANDOFF_BACKUP_HOLD`; capped at 30 s by the backup,
  `BACKUP_HOLD_MAX_MS`); the receiver's first append ends the hold. It is
  sound because it only delays a seal (liveness, like every timeout of
  M9): nothing is acknowledged without a write-all ack either way, a
  holder that never comes back is still sealed 15 s later, and only the
  backed holder at its own epoch can ask (the P2P layer drops a hold in
  another node's name). Not covered: an `ack=s3` lease's fast takeover by
  a non-backup (no such lease on the CSI pools). Proof: authority
  `a_backup_hold_defers_the_seal_until_the_successor_or_its_end`, net
  `a_backup_hold_reaches_the_backups_service`. On kind the hold went
  unused in the fix round's runs: there the controller's engine pod held
  the pool's root lease and the node pod was its *delegate* for the
  volume's subtree (and its backup); the pre-open below shrank the
  silence under the 1.5 s budget anyway (no seal in any run).
- **The standby pre-opens (37-k5a fix round).** The receiver's engine
  start is the pause the sessions' callers see, and most of it needs no
  state dir: a standby pre-opens, on a thread of its own while it waits,
  the backend client, `meta.json` and the conditional-write probe, and
  binds the P2P endpoint (`Engine::preopen`; under the node key the old
  engine still uses — harmless, as nobody can reach the new endpoint
  before its start publishes it, relays off; an E2E filesystem binds at
  start, its topic needing the keyring). The start then only reloads
  `meta.json`, opens `meta.db`, publishes its address and reads the
  registry. Measured on kind (host load 60-150, the three fix-round
  runs): `meta.db` open 163 / 843 / 90 ms, registry 34 / 49 / 69 ms, P2P
  5 / 5 / 4 ms, every other phase under 10 ms, the standby's wait from
  the seal to the lock 253 / 609 / 1055 ms (the old engine's commit —
  views closed, replica synced — and its exit); the plugin's `elapsed`
  342 ms / 1.35 s / 733 ms and the busy writer's longest call overlapping
  the handoff 292 ms / 1.29 s / 666 ms, against 2.1-9.0 s before (P2P
  bind 4.5 s, `meta.db` 2.5 s). §8's 2 s target is met on this host; what
  is left is `meta.db`'s open and the old engine's commit and exit, both
  load-bound. K5b's p99 should still watch the old engine's exit (the
  lock wait above), which nothing pre-opens.
- **Views detach concurrently (37-k5a fix round).** `Prepare` detaches
  up to 8 views at once (`handover::detach_targets`, shared with `daemon
  --upgrade`): each view's session, drain, barrier and export are its
  own, so the first view stopped no longer waits for every later view's
  drain and publication. A failure still serves every detached view
  again in place, once all were tried.
- **For K5b: a restarted delegate's first writes wait out its
  delegation.** In the fix round's kind runs the node pod was the
  delegate of the volume's subtree (M11) under the controller's engine
  pod. The receiver does not carry the delegation over: its first
  forwarded mutations back off against the root until the root reclaims
  the unrenewed generation at its expiry (~4 s; `reclaiming an unrenewed
  delegation`), then re-delegates. A `fsync` right after the resume took
  3.8 s / 4.7 s / 3.4 s — after the gated handoff window, but a call K5b's
  writers will see. K5b's options: the sender gives its delegations back
  before stopping (a delegate-side release), or the receiver re-adopts
  them as it re-adopts the lease.
- **The pre-commit drain still publishes.** `Prepare`'s detach runs the
  view's `sync_view` barrier: open write sessions are published
  write-through (their chunks uploaded) and the journal made as durable as
  `fsync` makes it. That is bounded by the dirty data since each file's
  last close or `fsync`, and it is part of the pause K5b measures; a
  bucket that is down fails the prepare (rollback), never the commit.
- **After the commit, nothing gives the sessions up.** From the marker on,
  the standby's descriptors are the only ones: it refuses `Abort`, and its
  seal deadline no longer applies (it waits for the old process to exit
  however long that takes). Before the marker, the seal deadline is set
  past the old engine's own (which refuses a commit after it), so a
  standby never gives up on a commit that can still come. The plugin's
  wait for `Resumed` after the commit has its own bound
  (`engineProfile.handoff.resumeTimeoutMs`, 300 s) and ends early only on
  the replacement's `Failed` or its pod ending (deleted, terminated, a
  container restart); past the bound the handoff is *unresolved* and the
  replacement stays pending — never deleted on a timeout — and is adopted
  once it serves. A served replacement whose adoption fails (readiness, a
  dial, the API server) is likewise retried, never retired.
- **No partial split across two pods.** §8's "some views on the new pod,
  the failed ones back on the old" needs two engines on one state dir; one
  engine owns every view of a unit, so a view that fails to resume ends.
- **Phases.** `node.handoff{target: Socket}` carries `phase`: the sender's
  `Prepare` (quiesce, drain, export; the old engine aborts by itself once
  `deadline_ms` passes without a commit, so a plugin dying half way stalls
  nothing for long), `Transfer` (records and `/dev/fuse` descriptors over a
  socketpair the plugin reads, `constellation_control::handoff_wire`),
  `Commit`, `Abort`; the receiver's `Receive` (every record and descriptor
  on a second socketpair: a handle table of any size, nothing rides in a
  control frame), `Seal`, `Status`, `Abort`. `daemon --upgrade` (`target:
  Exec`) takes no phase and is unchanged.
- **What the drain timeout bounds.** §8 wanted ops past the drain deadline
  carried in the handle table. `drain_timeout_ms` bounds only the reads and
  `fsync`s the engine answers off its FUSE workers (deferred); past it the
  detach refuses and the session resumes in place. fuser's detach joins
  its workers without a bound, so an op held *on* a worker (a write behind
  the backpressure barrier, a forwarded mutation, a slow S3 request) holds
  `Prepare` as long as it lasts: the plugin's step timeout gives up, sends
  `Abort`, and the old engine records it — the prepare then serves its
  sessions again the moment it ends (or if it outlived its own deadline),
  instead of parking them until the watchdog.
- **K0's gaps.** Gap 1 and 2 above. Gap 4: `detach` clears `O_NONBLOCK`
  before the descriptor leaves, and the stock `FUSE_INIT` path
  (`MountSource::PreopenedFd`) refuses a non-blocking descriptor or one
  with nothing to read (a handed-over connection never sends `FUSE_INIT`
  again); in the plugin, relayed descriptors go only to `Receive`.
- **Timing.** Drain 5 s, the steps up to the commit 30 s (§8's defaults;
  K0 showed a 10 s pause only delays callers), the old engine's own
  deadline 5 s past that, the seal's 10 s past the old engine's, the
  post-commit resume 300 s; the old engine exits 50 ms after answering its
  commit; the transfer writes its records outside the sender's state lock
  (so `Status` and `Abort` answer while a slow reader holds the stream;
  an `Abort` landing mid-write fails the transfer, and no commit can
  follow). 3 attempts per pod and desired spec, then the §8 fallback: the
  old pod keeps serving (an event, `constellation.dev/handoff-fallback`,
  `constellation_csi_handoff_fallback_total`; logged once) until it ends
  and the republish restages its volumes on the new spec.
- **For K5b: the gated pause is client-visible.** From the drain's start
  (the old engine receives `Prepare` and stops reading) until the first
  request is served by the new pod — what a writer sees as its longest
  call. It includes the drain (and its publication), the relay, the old
  engine's sync and exit, and the new engine's start; not the replacement
  pod's own start (image pull, process start), which happens before the
  drain. `elapsed` in the plugin's log (prepare sent to `Resumed` seen) is
  an upper bound of it; the root test `a_busy_write_back_writer_crosses_a_handoff`
  and `tests/csi/k5-handoff.sh`'s busy writer report it as the longest
  call.
- **Rollout trigger.** Each node pod carries the fingerprint of the engine
  settings it was made from (`constellation.dev/engine-config`: image, pull
  policy, resources, log level); a node plugin whose own settings (chart
  values, through its DaemonSet) differ rolls its drifted pods one at a
  time, at start and every 30 s. Replacements alternate between two socket
  slots (`control.sock`/`handoff.sock`, `-b` variants) and are named
  `…-g<generation>`; a pod serving no staged volume is simply deleted. A
  standby has no control socket, so the pod's startup and liveness probes
  also take an answer on its handoff socket (`control-relay --ping
  --or-socket`); readiness does not (a standby is not `Ready`).
- **The unit gate.** Stage, unstage, publish, unpublish,
  `NodeGetVolumeStats` and `NodeGetVolumeHealth` hold their unit's gate
  shared; a handoff holds it alone from `Prepare` to the end of the resume
  wait. So a kubelet RPC waits for the handoff instead of meeting a staging
  mount that does not answer.
- **Not rolled: controller-owned engine pods** (K6/K7). The controller's
  per-pool engine pods (K2) serve no FUSE session, so a chart upgrade does
  not hand them over: they keep the old image until they are recreated
  (the controller's idle GC, an operator's delete). K6/K7 must decide how
  they follow an upgrade (a plain delete-and-recreate suffices: they hold
  no mounts).

### K6 — Credentials, security, drain, purge, GC

- `EphemeralSecretStore` wiring for all three `CredentialSource` variants,
  secret-rotation scenario, the `control-acl.toml` service-principal grant,
  PodSecurity posture finalized, engine-pod idle-GC and node-drain
  (`node.leave`) implemented per §"Engine-pod lifecycle", **the controller
  purge worker and controller-owned engine pod implemented per settled
  decision 19 and §"Deletion and purge"** (rate limiting, `/.trash/`
  listing, GC interaction).
- **Secret RBAC (decided at 37-k1b review).** The K1 chart grants **no**
  `secrets` access: sidecars/kubelet resolve the `*-secret-name` references
  and pass the bytes in the request, and nothing before K6 reads a Secret.
  K6 adds exactly what the `Refreshing` Secret watch needs (the referenced
  Secrets, e.g. a namespaced `Role`/`resourceNames` per pool namespace),
  never a blanket cluster-wide `secrets` get — §"Security" RBAC above.
- **Durability default where node disks are ephemeral (recommendation
  from plan 39b, for K6/K7 to adopt).** Since plan 39b an `fsync` under
  the default `--fsync-mode local` puts the file's chunks in S3, but the
  file's *metadata* (its latest manifest, a rename, an unlink) is only in
  the node's local journal until the journal ships, so a node whose disk
  goes away with it (an instance-store volume, an `emptyDir` engine-pod
  state dir, a spot node reclaimed, a node pool scaled down without
  `node.leave`) loses acknowledged, `fsync`ed metadata: that node's disk
  is the only copy until the journal ships. So the StorageClass default
  should be `--fsync-mode s3` (an `fsync` also ships the journal), or the
  pool filesystem created with `ack_policy = s3` (every acknowledged
  mutation is in the bucket first), wherever the engine pods' state is
  not on a durable, re-attachable volume; `local` stays a per-class
  opt-in for clusters whose node disks outlive the node (and for scratch
  classes). `fsync`'s cost under `s3` is one extra journal ship per call,
  which K7's parity lane should measure against `local`.
- **K6b: idle GC never trusts `last-view-count` alone (37-k3b review).**
  The annotation is best effort: the node plugin logs a failed or
  timed-out (10 s) patch with a `warn` and moves on, so it can stay stale
  until the unit's next stage or unstage. Before collecting an engine pod
  that the annotation (with `idle-since`) shows idle, the GC re-checks
  the pod with `view.list` and keeps it if it serves any view.
- **K6b: registry churn of rescheduled controller-owned engine pods
  (37-k6a review).** The controller-owned engine pod keeps its state on
  `emptyDir`, so every reschedule rejoins the cluster as a fresh node and
  leaves a dead node record in the registry. K6b's GC/purge should expire
  those records (or give the pod a stable identity) so they do not pile up.
- **Gate:** CONVENTIONS gates; the secret-rotation and node-drain
  `k8s-scenario`s pass; the "trash purge under load" `k8s-scenario`
  (§"Testing") passes, including the large-many-small-files trashed volume
  case; a `kube-bench`-style PodSecurity check confirms the
  node-plugin/engine-pod/controller-owned-engine-pod privilege split matches
  §"Credentials and security" exactly (no accidental privilege
  broadening).

### K7 — Full e2e, parity lane, packaging, docs

- Kubernetes external storage e2e (`e2e.test -storage.testdriver`) full
  suite green; `linux-csi` parity lane wired into `tests/parity.py` with
  the `[[expect]]` additions from §"Testing" (landed in plan 31's file by
  whichever plan's session gets there second, per that plan's own overlap
  rule — recorded here as a dependency, not assumed already done); Helm
  chart finalized with `helm test`; container images published; the
  `seLinuxMount` question from settled decision 13 revisited with a
  concrete recommendation; **the static-provisioning and human-CLI-mount
  `k8s-scenario`s pass; the shard-routing `k8s-scenario` passes; the how-to
  guide publishes concrete sharding guidance derived from K0 Track B's
  measurement** (§2.3, §"K0 results").
- **Gate:** every CI job in §"CI" green; the full `k8s-scenario` suite
  green; `PROGRESS.md`/`TESTING.md` updated; the how-to guide for deploying
  Constellation as a Kubernetes CSI driver exists, and covers pool vs.
  dedicated layout selection and static provisioning.

## 16. What plans 32 and 33 provide for this plan

Both are already specified in those plans; this section only records the
contract this plan relies on.

- **Plan 32: snapshot holds with owner namespaces.** Its `SnapshotRecord`
  carries `held_by: Option<String>` (`csi:<VolumeSnapshotContent uid>`,
  `user:<name>`, `policy:` reserved), set with `snapshot hold --by <owner>`
  or the `snapshot.create{hold}` method. Held snapshots are invisible to
  policy expiry and pruning, and the simulator and `snapshot ls` show them
  as held by `csi`. `CreateSnapshot` sets the hold; `DeleteSnapshot`
  releases it, then deletes.
- **Plan 32: `size_bytes` = REFER.** Plan 32's coordination section settles
  REFER (the logical size the snapshot presents), which matches the CSI
  spec's intent for `size_bytes`: guidance on how much space a volume
  restored from the snapshot needs.
- **Plan 33 U1: service principals.** `control-acl.toml` grants of
  `kind = "service"` match a container uid on a given control socket and
  give the CSI plugin the `operator` role. Audit entries record
  `principal.kind = "service"` and the PV name.
- **Plan 33 UI.** Screen 3 shows CSI-provisioned views and filters on the
  `pv`/`pvc`/`namespace`/`pool`/`shard` labels — extended, per the
  coordinator brief's "Plan 33 addition," to show the pool hierarchy
  (`StorageClass` → pool filesystem → shards → volumes) with per-volume
  capacity vs. used, snapshots, and trash/purge status, with admin actions
  limited to safe ones (inspect, purge-now of trash); volume
  creation/deletion stays with Kubernetes, never done from this UI. Screen
  5 marks `csi:`-held snapshots as externally owned; deleting one needs
  admin and an explicit override.

## 17. Risks

- **The fuser session-resume patch is this plan's single largest technical
  risk**, and it is treated as such: K0 is a dedicated, timeboxed spike
  with pre-agreed fallbacks, not an assumption baked into K1-K7's design.
  Even in the fallback case (§K0), the rest of this plan (K1-K4, K6-K7)
  delivers a working, if less resilient, CSI driver — the plan is
  structured so a K0 partial failure degrades the product, it does not
  block shipping it.
- **Per-PV FUSE sessions (settled decision 5) mean fd count scales with PV
  count per node, not with filesystem count.** A node with many small PVs
  of the same filesystem pays one `fuse_mount_fd`/kernel connection per PV
  even though they share one engine's cache — this is a real resource cost
  (each FUSE connection has kernel-side buffers) traded for the
  bind-mount-fan-out simplicity of settled decision 5. If K3's testing shows
  this is a problem at realistic PV-per-node counts, the alternative (one
  shared FUSE mount per filesystem, with per-PV subtrees exposed as
  directory entries under it, bind-mounted per PV the same way) is a
  documented follow-up, not a redesign of the fd-passing/handover mechanism
  itself, which is orthogonal to this choice, and is deferred rather than
  gating K1-K7 on it since the primary integration story does not depend
  on it.
- **The engine-pod idle-GC TTL (§"Engine-pod lifecycle") trades memory for
  thrash-avoidance with a fixed default.** A cluster with very bursty PV
  attach/detach patterns might want a shorter TTL (less idle memory); a
  cluster with expensive S3-cold cache rebuilds might want a longer one.
  Exposed as a Helm value, not hardcoded, but the default is a judgment
  call K7's real-world testing should revisit.
- **PodSecurity `privileged` at the namespace level (§"Credentials and
  security") is coarser than ideal.** A future Kubernetes admission-webhook-based
  per-pod exception (rather than namespace-wide `privileged`) is a
  plausible K7-or-later hardening step, not attempted here because it adds
  a whole new admission-control component for a security improvement this
  plan's engine-pod-unprivileged design already captures most of the value
  of.
- **CSI spec alpha fields.** `SINGLE_NODE_MULTI_WRITER`/
  `SINGLE_NODE_SINGLE_WRITER` are marked alpha in the upstream
  `csi.proto` (VERIFIED via this session's spec fetch) even though
  Kubernetes' `ReadWriteOncePod` built atop them has been GA since 1.29 —
  this plan tracks the *Kubernetes* stability level (GA, safe to depend on)
  rather than the CSI spec's own alpha marking, which lags Kubernetes'
  adoption; if a future CSI spec revision changes this enum's shape,
  K7-or-later revisits it, but this is not expected to be a practical risk.
- **Pool blast radius (§2.3, settled decision 16).** Pooling's whole value
  proposition — one shared cache, one shared commit chain — is also its
  risk concentration: a bug that corrupts one pool's metadata, or an
  engine-pod incident that exhausts the pool's shared `ResourceBudget`,
  affects every PV in that pool at once, not just one PVC's worth of blast
  radius the way `layout: dedicated` or a pure filesystem-per-PV design
  would contain it. This is a deliberate, stated trade-off (§2.3's
  isolation column), not an oversight — the mitigation is organizational
  (use `dedicated` or a separate pool per real isolation boundary, settled
  decision 16), not a technical one this plan can add without undoing
  pooling's benefits. Documented prominently in the Helm chart's README,
  not just here.
- **The shared commit chain is a real throughput ceiling, not just a
  theoretical one (§2.3's "Shards").** Unlike a dedicated or
  filesystem-per-PV layout, a pool's metadata operations for *every* PV
  serialize against the same commit chain; K0's Track B measurement (§15)
  quantifies this, but the number itself is a risk until measured — if it
  turns out to be lower than expected for realistic multi-tenant workloads,
  the default `shards: 1` might need a higher default, or the how-to guide
  might need to recommend sharding much more aggressively than this plan
  currently assumes. Tracked as a K0 finding to revisit, not a fixed
  number baked into this plan ahead of measurement.
- **Purge cost of huge volumes (§"Deletion and purge").** A deleted PV
  holding millions of small files makes its `browse.delete{recursive:
  true}` a genuinely slow metadata operation; the purge worker's rate
  limit contains the *blast radius* (other pools' purges aren't starved)
  but does not make the delete itself fast. A pool whose users routinely
  create and delete huge volumes may see `/.trash/` backlogs and delayed
  chunk reclamation as a result — surfaced via the
  `constellation_csi_purge_pending_total` metric (§"Observability") so an
  operator sees it building up rather than discovering it as "space isn't
  coming back." An incremental/resumable purge (§"Deletion and purge") is
  the documented follow-up if this proves common in practice.

## 18. Definition of done

The CONVENTIONS gates, PLUS:

1. **Every milestone's gate (K0-K7) met**, including the 20-run
   zero-`ENOTCONN` bar in K5 — this is not satisfied by a single green run.
2. **CI green**: `csi-unit` in `ci.yml`; `csi-sanity`, `kind-e2e`,
   `upgrade-under-load` in `nightly.yml`; the `linux-csi` parity lane
   reporting zero unexplained differences once plan 31's
   `tests/platform-parity.toml` carries the `[[expect]]` additions from
   §"Testing".
3. **Kubernetes external storage e2e** full suite green against the driver.
4. **Helm chart** installs cleanly on a bare kind cluster and passes its own
   `helm test` hook.
5. **Linux reference lane unchanged**: pjdfstest 8798/8798 and the harness
   full matrix pass — this plan adds a new frontend of the *engine's*
   filesystem (mounted via a different orchestrator), it does not touch
   `constellation-frontend-fuse`'s own behavior, so nothing here should ever
   move that baseline.
6. **Docs updated**: `PROGRESS.md` gets a plan-37 section with the K0
   results matrix (both tracks); `TESTING.md` covers the `linux-csi` lane,
   `csi-sanity` invocation and the `k8s-scenario` harness mode; a how-to
   guide for deploying the Helm chart exists and covers pool vs. dedicated
   layout selection, sharding guidance (from K0 Track B), static
   provisioning and the human-CLI-mount safety rules (settled decision 18);
   the plan-31 `browse.*` addition and the plan 32/33 needs this plan
   records (§"What plans 32 and 33 provide for this plan") are filed as
   tracked follow-ups for those plans' own sessions, not silently left
   implicit.
7. **Pool scenarios pass**: many-PVs-in-one-pool, clone/restore
   within-pool and cross-pool refusal, trash-purge-under-load, static
   provisioning, human-CLI-mount-alongside-CSI, and shard-routing
   `k8s-scenario`s all green (§"Testing") — not just the pre-pooling
   scenario set.
8. **Report**: per-job CI pass/fail tallies, the K0 results matrix
   (handover Track A *and* metadata-throughput Track B), the K5
   handoff-duration p50/p99 and the 20-run `ENOTCONN` tally, the pool
   scenario results, and the parity summary.

## K0 results

Track A (this section) was run on 2026-10-01 against kernel `7.0.0-31-generic`
(Ubuntu 24.04). Nobody has root on the host, so the probes ran as root in a
`--privileged --device /dev/fuse` container on that same kernel, and question 5
ran in a three-node `kind` v0.33.0 cluster. The artifacts:

| what | where |
|---|---|
| The handover probe (A → B → C, `SCM_RIGHTS`, `detach`/`resume`, writer + reader + fio throughout) | `crates/frontend-fuse/examples/handover_probe.rs` |
| The `NodeStageVolume` probe (`fuse_mount_fd` here, `view.mount{PreopenedFd}` to a real daemon) | `crates/cli/examples/stage_volume_probe.rs` |
| Runners, full write-up, raw JSON | `bench/fuse-handover-probe/` (`README.md`, `RESULTS.md`, `run.sh`, `stage-volume.sh`, `results/`) |
| The kind cluster config, as verified | `tests/csi/kind-config.yaml` |

**Track B (question 6) ran 2026-10-01, independently of Track A, on the
same host.**

| # | Prediction | Observation | Consequence taken |
|---|---|---|---|
| **6** (Track B) | A latency *knee* in the `CreateVolume` metadata-op sequence as concurrency/subtree count rise, dominated by the shared commit chain. | **Both a knee and, on top of it, a failure-rate cliff — and both are dominated by one traced call (`quota.set`'s existing full-tree durability barrier), not by the commit chain generally.** The knee: successful-sequence p99 at 10,000 subtrees rises super-linearly through the c=16…64 region — 22.3 ms at c=16 → **89.0 ms at c=32** (4.0× for a 2× concurrency step) → 171.1 ms at c=64 → **635.9 ms at c=256** — i.e. hundreds of milliseconds, not tens, so `CreateVolume`'s gRPC deadline must be sized against that. The cliff: in the same region most calls stop completing at all, failing with `journal not shipped: no lease` (a control RPC failure, not added latency). Error rate over 10,000 sequences per level, run 2 (the full grid in one command): c=1 **0%**, c=4 **0.02%**, c=8 **0.20%**, c=16 **0.41%**, c=32 **9.61%**, c=64 **71.7%**, c=256 **94.5%** — but the magnitude is host-dependent, not a property of the code: across four runs c=8 ranges 0.16-**12.6%** and c=64 ranges 51.6-91.4%, tracking host load (recorded per step). Failures are *cheap*: a failing `quota.set` returns in 20.4 ms p50 / 47.3 ms p99 at c=64. `node.ping` on the same grid runs at 56.6k-348k ops/s with p99 from 0.08 ms (c=1) to 2.6 ms (c=256), ruling out the control socket/dispatch path and the single-connection client shape. Traced to the code: every `CreateVolume` ends in `quota.set`, which calls `EngineControl::set_quota` → `self.snapshot_barrier("/")` (`crates/engine/src/control/service.rs:1082`) → a `Control::Barrier` round-waiter (`crates/authority/src/core/mod.rs:1864-1869`) that fails with this exact message if the node's *whole* journal backlog isn't zero by the end of that sync round (`crates/authority/src/core/jobs.rs:1999-2005`) — and the grid records `lease_held: true`/`lease_lost: false` on every step, so no lease was ever actually lost. `browse.mkdir`/`browse.xattr` never call a barrier (`crates/engine/src/control/browse.rs`); `failures_by_op` is `{quota: n}` on every failing step, with zero mkdir/xattr failures in 110,000 sequences. | **The guidance is a mechanism, not a safe number, and it is a concurrency bound rather than a PV-count bound.** No PV-count threshold exists (c=1 is clean at every pool size, including at the heaviest host load measured) and **no safe concurrency above 1 could be measured** — the per-call failure probability is the probability that the node's journal backlog reaches zero inside the round its `quota.set` waits on, which depends on host throughput as much as on concurrency. So **K2 must make `CreateVolume` tolerate this (retry the trailing `quota.set`, cheap at ~10-60 ms per failed call; map an exhausted retry to a retryable status) instead of relying on a documented concurrency cap**; sharding still buys throughput (~1.2-1.4k successful seq/s per pool) but does not raise the per-pool failure threshold. As a pre-K2 stopgap, keep in-flight `CreateVolume`s per unsharded pool in the single digits and still expect a fraction of a percent to fail. §"K0 Track B — pool metadata-throughput ceiling" below has the full tables, the four measurement caveats (including that the `quota.set` measured is today's filesystem-wide `{max_bytes}` RPC, not §5's subtree-scoped shape — K2 must re-measure when that exists), the attribution and the K2/K6 gap. |

### Track A

| # | Prediction (§8/§12/§15) | Observation | Consequence taken |
|---|---|---|---|
| **1** | Stock `Session::from_fd` on an already-initialised connection replies `EIO` to a real request and then fails hard. | **Reproduced exactly.** With a client's `statfs` waiting in the kernel queue (`waiting` = 1), `from_fd` returned `InvalidData`, *"Received non-init FUSE operation during handshake"*, and the client's syscall came back **`EIO`** (errno 5), 1.3 ms after the descriptor reached the second process. Also found, not predicted: with *nothing* waiting, `from_fd` does not return **at all** and burns 4983 ms of CPU in a 5000 ms window (**99.6 % of a core**). The cause is measured, not inferred: clearing `O_NONBLOCK` on the handover descriptor and changing nothing else leaves the same non-return at **0.0025 %** of a core — an ordinary blocking `read`. `SessionControl::detach` hands back a descriptor that is still `O_NONBLOCK` (an armed session sets it on the open file description, which travels through `SCM_RIGHTS`), and `fuser`'s `receive_retrying` retries `EAGAIN` immediately. | The patch is validated against a reproduced failure, as the question asked: `FuseSession::resume`/`from_fd_resumed` exists for exactly this and is unaffected (it skips the handshake, and an armed session `poll`s before reading). **For K5:** the production path must never reach stock `from_fd` with a handover descriptor; a misuse is a spinning core, not an error, so K5's resume path gets an explicit guard rather than relying on the error. |
| **2** | The vendored resume patch serves ordinary read/write/getattr with no re-`INIT`. | **Yes.** 44 handoffs in one chain, through 45 distinct server processes, with writers and readers never stopping: every `FuseSession::resume` returned `Ok`, and the **resumed session's own** `negotiated_init()` — read back out of the session, not echoed from the request that carried it — equalled the detaching session's on all 44 handoffs. *No re-`INIT`* is by construction: `Session::from_fd_resumed` (`vendor/fuser/src/session.rs:406`) stores the carried `NegotiatedInit` and never calls `handshake()`, so there is no path on which a `FUSE_INIT` could be written; what the run adds is that the connection then **works** — after the last resume, `getattr`, a 4 KiB `O_DIRECT` `pread` of a writer's file (0 bytes differ from the pattern written before the handoffs), a write through a **freshly opened** handle and `readdir` were all correct. Over the whole run: 5.0 M writer ops / 329 GB and 1.2 M reader ops / 79 GB with **0 errors and 0 byte mismatches**, every reader operation answered by a server process over the handed-over connection (the readers are `O_DIRECT`, so no read was served from the page cache), and fio (`randrw`, 64k, 2 jobs, 60 s, spanning ~40 handoffs) exited 0 with `error: 0`. The probe *asserts* this: an error, a mismatch, a failed post-resume check or a non-zero fio status exits non-zero. | **Handover works as designed → proceed to K1-K7 as planned**, per §15's first pre-agreed consequence. `vendor/fuser`'s patch set stays a plan-31 C4 deliverable; nothing in §8 steps 1-5 needs redesigning. |
| **3** | Under 2 s for the fd-passing round trip (§8 step 6's target). | **Three orders of magnitude under it at the median, two at the worst handoff.** Over 40 back-to-back handoffs: round trip (detach request → descriptor in hand → `SCM_RIGHTS` to the next process → it is serving) **p50 0.82 ms, p90 1.16 ms, max 21.5 ms** — at n = 40 the p99 *is* the maximum, so it is reported as one. The `SCM_RIGHTS`-send-plus-reconstruction leg alone is p50 0.19 ms / max 0.32 ms, and the detach leg (which contains §8 step 2's drain and `Vfs::sync_view`) p50 0.62 ms / max 21.2 ms. First client op after the resume: p50 0.006 ms, max 109 ms. At 80 concurrent client threads: round trip p50 1.29 ms, p90 2.01 ms, max 12.3 ms. One handoff of the 40 is the entire tail (that one: detach 21.2 ms, first op 109 ms, longest client syscall in the window 239 ms); the other 39 are ≤ 1.4 ms round trip. The host carried an unrelated load average of 20-55 across 32 CPUs, which the probe cannot separate from a handoff cost. | §8's 2 s target stands with enormous headroom even at the measured maximum, so **K5's timing budget is dominated by step 2's drain (data-dependent) and by pod scheduling, not by the transfer**. K5 gates the end-to-end `node.handoff` p99, not this leg, and sets it against the maximum rather than the median — a loaded node's tail is tens of milliseconds, not sub-millisecond. Note the probe pre-starts the next server before the clock, as a new engine pod would already be running; a cold pod start is a Kubernetes cost K5 measures separately. |
| **4** | The kernel queues in-flight and new requests while nobody reads; bounded queue depth might make callers block, which is why step 2's drain has a timeout. The open question was whether it blocks or **errors** at realistic depth. | **Blocking only — never an error, at any depth or pause length tested.** With nobody reading `/dev/fuse` for 0.5 / 2 / 5 / 10 s, the writer's longest `pwrite` *and* the reader's longest `pread` were the pause + 1.3-2.4 ms (12 clients) or + 1.7-5.4 ms (80 clients), and the error count was **0** every time. `waiting` went 6-13 at 12 clients; at 80 clients it reached **80** — every client outstanding at once, above `congestion_threshold` (48) and above `max_background` (64), which bound *background* requests (readahead, writeback) and not the synchronous ones these clients issue — and still nothing failed: congestion throttles, it does not error. Both directions wait out the pause and are answered after the resume (the readers are `O_DIRECT`, so every one of their `pread`s had to cross the paused connection). | **K5's protocol timing does not have to change.** A 10 s stall is twice §8's default `--handoff-drain-timeout` (5 s) and a third of `--handoff-total-timeout` (30 s), and it is invisible to a caller except as latency, so the drain timeout stays a *liveness* bound rather than a correctness one. K5 should still keep the total timeout: a stall is only harmless while it ends. |
| **5** | `extraMounts` puts `/dev/fuse` in the kind node (REPORTED, `kubernetes-sigs/kind#2540`), and the pod additionally needs `privileged: true` (REPORTED, two independent projects). | **The pod half is VERIFIED and is in fact stronger than stated; the node half is VERIFIED differently than stated.** A privileged pod on an `extraMounts` worker ran the whole chain (5 handoffs + a 1 s pause, 176 k writer ops, 92 k reader ops, 0 errors, 0 mismatches). An unprivileged pod cannot: with no device it gets `ENOENT`; with the device bind-mounted in by `hostPath` it gets **`EPERM` on `open("/dev/fuse")`**; with `hostPath` **and `SYS_ADMIN`** it *still* gets `EPERM` (the capability was genuinely in force — that pod could mount `fusectl`, the plain one could not). So bind-mounting the device in is not a substitute for `privileged: true`. The correction: on this host the `extraMounts` were **not** what made `/dev/fuse` available — kind runs node containers `--privileged`, runc populates a privileged container's `/dev` with every host device, and the control-plane node (deliberately left without `extraMounts` in the committed config, as a control group) had a working `/dev/fuse` too. | `tests/csi/kind-config.yaml` is committed with the `extraMounts` kept: they are harmless, they make the device present deterministically instead of as a side effect of runc's privileged-container behaviour, and they remain the documented pattern for rootless and restricted runtimes. The node-plugin `DaemonSet` keeps `securityContext.privileged: true` — K0 shows there is no weaker configuration that works. **A GitHub-hosted `ubuntu-latest` runner could not be tested from here**, so §15's pre-agreed fallback is taken as written: **`kind-e2e` and `upgrade-under-load` run on a self-hosted runner with FUSE support until a hosted runner is verified**, while `csi-unit` and `csi-sanity` stay on hosted runners (K3b's CI job, §13). `results/k0-question5-kind.txt` predates the probe's reader fix and the cluster is deleted, so it was not re-run: what question 5 asks — is the device there, and can a pod use it — does not depend on how the probe's readers reach the filesystem, and its in-pod handoff legs (round trip p50 0.53 ms) agree with the host runs. |

### Track B — pool metadata-throughput ceiling (37-k0b, 2026-10-01)

**Setup.** `crates/harness/src/csi_meta_ladder.rs` (`harness csi-meta-ladder`):
one `constellation` daemon, one pool filesystem, docker floci + toxiproxy,
every op through the control socket (`browse.mkdir`/`browse.xattr`/
`quota.set` — never through the FUSE mount; `EngineControl::browser()` opens
its own internal view, so no filesystem op in this grid crosses the kernel).
Concurrency ladder 1, 4, 8, 16, 32, 64, 256 — all seven are in
`CONCURRENCY_LADDER`, so one plain `harness csi-meta-ladder` reproduces every
row below without env knobs. At each concurrency level one pool filesystem
grows from 0 to 10,000 subtrees through checkpoints 100/1,000/5,000/10,000,
measured per checkpoint. Raw JSON + logs: `bench/csi-metadata/results/` (see
its `README.md` for the re-run commands and knobs).

Four caveats that bound how far these numbers travel, all of them recorded
per step in the JSON:

1. **The `quota.set` measured is not §5's.** The control protocol has no
   subtree-scoped quota: `SetQuotaParams` is `{max_bytes}`, a filesystem-wide
   cap, and §5's `quota.set{subtree, bytes}` does not exist yet. This grid
   measures the RPC that exists today. It matters for reading the result
   below, because the whole failure mode *is* that RPC's unconditional
   whole-filesystem barrier: **a future subtree-scoped `quota.set` that
   barriers only its own subtree (or not at all) may not have this failure
   mode at all**, and K2 should re-measure once that RPC exists rather than
   assume the cliff carries over.
2. **Host load is part of every number here.** This host is shared with other
   agents' builds; its 1-minute load average moved between 29 and 143 across
   the runs, and *within* run 2 it fell from 143 to 37 while the ladder
   climbed. `loadavg_1min_before`/`_after` and `host_cpus` are therefore
   recorded on every step, and no row is comparable with another run's row
   without them.
3. **The 100-subtree checkpoint is not a comparable data point** at the high
   levels: at c≥64 it completes in 0.06-0.15 s, far shorter than one sync
   round, so whether it shows 0% or tens of percent of errors is close to a
   coin flip on where the round boundary fell (run 2 c=64 → 25%, run 3 c=64 →
   0%, run 2 c=256 → 55%). Read it as a warm-up, not as "small pools are
   safe".
4. **A FUSE mount is live throughout**, because `Client::mount` is the
   harness's only way to start a daemon. No *measured* op goes through it,
   but `quota.set`'s `invalidate_quota_caches()` does touch a real view, so
   this is not a mount-free configuration — just one where the mount carries
   no traffic.

**There are two results, and they sit on top of each other: a super-linear
p99 knee *and* a failure-rate cliff, in the same concurrency region.** Run 2,
the whole grid in one command (`run2-grid-full.jsonl`; the "seq/s" columns are
`sum(volumes)/sum(wall_s)` over the level's four checkpoints, computed by
`csi_meta_ladder::summarize` so this table and the tool cannot disagree; the
`@10k` columns are the 5,000→10,000 checkpoint):

| Conc | Errors / 10,000 | Attempted seq/s | Successful seq/s | seq p50 @10k (ms) | seq p99 @10k (ms) | Failed `quota.set` p50 / p99 (ms) | Daemon CPU @10k | load1 @10k |
|---|---|---|---|---|---|---|---|---|
| 1 | 0 (0.00%) | 154.3 | 154.3 | 5.18 | 12.48 | — | 76.7% | 64.2 |
| 4 | 2 (0.02%) | 656.7 | 656.5 | 5.75 | 10.59 | 15.49 / 15.49 | 126.8% | 52.8 |
| 8 | 20 (0.20%) | 908.4 | 906.6 | 8.78 | 21.36 | 17.17 / 145.46 | 168.1% | 46.4 |
| 16 | 41 (0.41%) | 1183.1 | 1178.2 | 13.74 | 22.33 | 5.04 / 8.51 | 193.4% | 41.2 |
| 32 | 961 (9.61%) | 1422.9 | 1286.1 | 23.53 | 88.97 | 8.55 / 16.48 | 208.6% | 37.2 |
| 64 | 7171 (71.71%) | 1061.3 | 300.2 | 60.81 | 171.11 | 20.43 / 47.26 | 157.6% | 37.4 |
| 256 | 9451 (94.51%) | 1146.1 | 62.9 | 302.03 | 635.94 | 57.46 / 160.06 | 161.2% | 38.2 |

**The knee (what question 6 asked for).** Successful-sequence p99 at the
10,000-subtree checkpoint goes 22.33 ms at c=16 → **88.97 ms at c=32** (4.0×
for a 2× concurrency step) → 171.11 ms at c=64 → **635.94 ms at c=256**; p50
follows (13.74 → 23.53 → 60.81 → 302.03 ms). That is the super-linear rise
the question predicted, it is in the **hundreds of milliseconds**, not the
tens, and it lands in the same c=16…64 region as the failure cliff. The
earlier run 1 put the same break one step higher (p99 27.74 ms at c=32 →
534.30 ms at c=64, 19× for a 2× step); run 3 with its lower host load shows
the mildest version (30.39 ms at c=16 → 31.02 ms at c=64, with 86% of
sequences failing). So the knee's *location* moves with host load between
c=16 and c=64, but a knee is always there, and **K2/K7 must size
`CreateVolume`'s gRPC deadline against hundreds of ms, not tens**.

Two things to know before using those percentiles. First, they are
**survivorship-biased**: `sequence_p50/p99` cover only the sequences that
completed, which at c=64 is 28% of them and at c=256 is 5.5%. Second, the
sequences that failed are now timed too (`failed_op_*`, `failed_sequence_*`),
and they are *cheaper* than the successful ones at high concurrency — a
failing `quota.set` returns in 20.4 ms p50 / 47.3 ms p99 at c=64 (57.5 /
160.1 ms at c=256), and the whole failed sequence in 57.0 ms p50 at c=64
against 60.8 ms for a successful one. **A failure is fast, so retrying just
`quota.set` is cheap** — the number K2 needs for the gap below.

**Neither the control socket nor the client is the limit.** The `node.ping`
ladder runs on the same grid, over the same single shared connection and the
same task-per-concurrency-slot shape the `CreateVolume` grid uses, at the same
total op count (80,000 calls per level): **56.6k-348k ops/s** across all runs,
with zero errors, and p99 rising from **0.08 ms at c=1 to 2.55 ms at c=256**
(run 1's worst was 4.67 ms at c=256 — *not* sub-millisecond at the top of the
ladder, as an earlier version of this section said, but still two orders of
magnitude of headroom). Against the 1.2-1.4k sequences/s the `CreateVolume`
grid peaks at — 9.5-11.4k individual RPCs/s — the socket's framing, dispatch
and `spawn_blocking` handoff (`unary()` in `crates/engine/src/control/mod.rs`) are
nowhere near saturated, and neither is the harness's one-connection client.
Question 6's "is it the client?" check is therefore answered no.

**The failure rate is not a property of the code alone — it is a property of
the host.** Every run of this ladder, newest first, with the load average the
level ran under (`—` = level not run; run 1 predates the per-step load field):

| Conc | Run 2 (full grid) | Run 3 (subset) | Run 1 (first pass) | Run 1 reruns | Reviewer's rerun |
|---|---|---|---|---|---|
| load1 during the level | 143 → 37 | 35 → 29 | not recorded | not recorded | ~76 |
| 1 | 0.00% | — | 0% | — | — |
| 4 | 0.02% | 0.19% | 0.22% | — | — |
| 8 | 0.20% | 0.28% | 0.16% | — | **12.6%** |
| 16 | 0.41% | 0.88% | 1.11% | 0.20% | — |
| 32 | 9.61% | — | 7.94% | — | — |
| 64 | 71.71% | 86.00% | 91.38% | 57.66% | 51.6% |
| 256 | 94.51% | — | 87.57% | — | — |

Read across a row, not down a column: c=8 is 0.16-0.28% on three runs of this
host and **12.6%** on a fourth at load ~76, and c=64 ranges 51.6-91.4%. The
one robust ordering is that the rate grows steeply with concurrency even when
host load is moving the *other* way — in run 2, c=1 was measured at load
64-143 and still returned zero errors, while c=32-256 ran at load 37-41 and
failed 9.6-94.5%. **Concurrency is the driver; host throughput sets where the
cliff falls.** There is no concurrency level above 1 that was clean on every
host measured.

**Attribution — traced to the exact call, not inferred from the shape of the
curve.** Every `CreateVolume` sequence ends in `quota.set`. The handler,
`EngineControl::set_quota` (`crates/engine/src/control/service.rs:1080-1095`),
unconditionally calls `self.snapshot_barrier("/")` *before* writing the new
quota (line 1082) — a full-filesystem durability barrier originally meant for
snapshot/clone correctness (`snapshot_barrier`'s own doc: "force its pending
data + journal through before observing or publishing an immutable root", same
file, lines 37-39). `snapshot_barrier` sends `SyncRequest::Acquire` then
`SyncRequest::Barrier{ino: root}`, which the authority core registers as a
`Control::Barrier` round-waiter (`crates/authority/src/core/mod.rs:1864-1869`).
A round-waiter is resolved at the end of the *next* sync round; if the node's
**whole** journal backlog (not scoped to "/" despite the `ino` parameter —
`replica.journal_len()`, `crates/authority/src/core/jobs.rs:1963`) is still
nonzero at that point, every non-`PublishNow`/`Reintegrate` waiter in that
round — including this `Barrier` — is failed with the literal string
`"journal not shipped: no lease"` (`jobs.rs:1999-2005`), regardless of whether
a lease was ever lost. Three recorded artifacts back this up rather than
inference:

- `failures_by_op` is `{"quota": n}` on **every** step of both new runs that
  recorded a failure at all (and `{}` on the rest): zero `mkdir` and zero
  `xattr` failures across the two runs' 110,000 sequences — matching
  `crates/engine/src/control/browse.rs`, where `mkdir`/`xattr` call straight
  into `View::mkdir`/`xattr` with no barrier.
- `lease_held_before`/`lease_held_after` are `true` and `lease_lost_after` is
  `false` on every step of both runs, so the message's "no lease" is
  demonstrably not what happened — this is one uncontested node throughout.
- `journal_backlog_after` is nonzero on most high-concurrency steps (up to 45
  at c=256) while being 0 on the c=1 steps, which is the mechanism's own
  signature: concurrent `mkdir`/`xattr` traffic keeps the backlog from
  reaching zero inside the round a concurrent `quota.set` is waiting on.

There is no automatic retry — the control RPC fails back to the caller once.

**Pool size (PV count) is not the error-rate driver.** Within one concurrency
level, the error *rate* does not consistently climb with the subtree count:
run 2's c=16 goes 0% → 1.78% → 0.62% → 0% across its four checkpoints, c=4
goes 0 → 0 → 0 → 0.04%, c=32 rises (0 → 4.78% → 3.82% → 15.30%) and c=256 is
flat-high (55% → 93.9% → 96.5% → 93.8%); run 3's c=4 *declines*
(0 → 0.44% → 0.22% → 0.12%) and its c=64 is non-monotone
(0 → 82.1% → 72.1% → 99.6%). Averaged over runs there is no monotone PV-count
trend, and the earlier version of this section wrongly read c=64's raw error
*counts* (0 → 862 → 3496 → 4780) as a climb when the four checkpoints attempt
100/900/4,000/5,000 sequences — i.e. flat in rate. What *does* grow with pool
size is latency, mildly and linearly: at c=8, successful-sequence p50 goes
6.79 → 7.08 → 7.77 → 8.78 ms across the checkpoints, and at c=16,
9.98 → 10.42 → 11.96 → 13.74 ms. This only strengthens the conclusion below:
the bound is on concurrency, not on how many PVs the pool already holds.

**Consequence taken — the guidance is a mechanism, not a safe number.** The
question §2.3 asks ("how many PVs can one unsharded pool hold before an
operator should set `shards > 1`?") has no PV-count answer: a pool grown one
PV at a time was clean through all 10,000 PVs measured, at the heaviest host
load of the run and with nothing in the mechanism that would make 50,000
different, while a pool fielding 64 simultaneous `CreateVolume` calls fails
most of them at any PV count, including at 100. Nor can it be
answered with a *safe concurrency*, because the failure probability per
`CreateVolume` is the probability that the node's journal backlog fails to
reach zero within the sync round its trailing `quota.set` is waiting on — a
function of total concurrent metadata load **and** of how fast the host drains
the journal. On this host that put the first failures at c=4 (2 in
10,000) and majority failure between c=32 and c=64; on the same host at a
higher load, c=8 alone was at 12.6%. Therefore:

> **Sharding does not fix this and a concurrency cap cannot be published as
> safe. K2 must make `CreateVolume` tolerate the failure — retry the trailing
> `quota.set` (a failing call returns in ~10-60 ms, so retries are cheap) and
> map an exhausted retry to a retryable gRPC status — rather than rely on an
> operator keeping concurrency under a documented number.** Operators who want
> a stopgap before K2 ships the retry should keep in-flight `CreateVolume`s per
> unsharded pool in the **single digits** and still expect a fraction of a
> percent to fail; the only level that was clean on every host measured is
> **1**. Sharding (§6's example 2) remains the right tool for *throughput* —
> successful-sequence rate per pool plateaus around 1.2-1.4k seq/s, so N shards
> buy roughly N× that — but it does not raise the per-pool failure threshold,
> because each shard's `quota.set` barriers its own filesystem on the same node.

**Gap for K2/K6 (recorded, not fixed here — out of this session's scope,
same as plan 37's own rule for Track A's gaps below).** `CreateVolume`'s
error-mapping table (§5) has no entry for this failure: it is transient and
partial (the subtree and all six xattrs are already committed by the time it
happens; only the trailing `quota.set` barrier failed), so it should map to
something retryable (`ABORTED` or equivalent) rather than whatever
`Unavailable`/raw-passthrough mapping a naive implementation would give it,
and — per the guidance above — K2 should retry just the `quota.set` step
rather than failing the whole `CreateVolume`. When the subtree-scoped
`quota.set{subtree, bytes}` of §5 is built, K2 should re-run this ladder: if
the new RPC barriers only its own subtree, the cliff measured here may not
exist in the shape CSI actually calls. Separately: `set_quota`'s unconditional
whole-filesystem `snapshot_barrier("/")` is pre-existing engine behavior
(plans 22/31/32), not something plan 37 introduces, and it will produce the
same failure for *any* concurrent-write workload that calls `quota.set` while
mutations are in flight, CSI or not — worth flagging to whoever owns
`quota.set` past K2, though this session makes no engine change (per its
brief: "no changes to the engine's metadata path").

**K2 re-run (37-k2b review, subtree shape).** With `quota.set{subtree}`
built (a journaled xattr on the volume's directory, no barrier) the ladder
was re-run at c=64 over 10,000 sequences: **0 errors**, 1377 seq/s, p50
43 ms, p99 103 ms. The cliff does not exist in the shape CSI calls. The
controller keeps its bounded `quota.set` retry anyway (PROGRESS "Plan 37
K2").

### Gaps K0 found, for K5 and K3a (recorded, not fixed here)

1. **`node.handoff{target: Socket}` does not exist.** The daemon answers
   `Unsupported` — *"handing sessions to another process over a socket is plan 37's"*
   (`crates/cli/src/control.rs`). §8's protocol is therefore unexercised as a
   *protocol*: K0 drives `SessionControl::detach`/`FuseSession::resume` directly over
   its own `SCM_RIGHTS` socket. **K5 builds it.**
2. **§8's phases have no representation in the protocol.** `HandoffParams` carries
   `{target, views, drain_timeout_ms}` and no `phase`; `Prepare`, `Commit` and `Abort`
   (§8 steps 1, 6 and the rollback paths) exist only in prose. **K5 adds them**, or
   §8 is rewritten to the shape `Socket` actually needs.
3. **`view.unmount` hangs for a view attached with `MountSource::PreopenedFd`** — the
   shape `NodeUnstageVolume` needs. `NodeRuntime::remove_mount` asks the session to
   unmount, `SessionControl::unmount` refuses ("the mountpoint of a preopened FUSE
   session is unknown": nothing calls `FuseSession::set_mountpoint` on that path), the
   refusal is only logged, and `remove_mount` then blocks in `thread.join()` on a
   session nothing has ended. The view is removed only once somebody unmounts the
   kernel mount by path. **K3a** either carries the mountpoint in `ViewMountParams` for
   a preopened fd, or makes `view.unmount` end the session without unmounting; either
   way the call must answer. (The node plugin unmounting by path is the right division
   of labour regardless — it made the mount.) **Closed by 37-k3a, both ways:**
   `MountSource::PreopenedFd{mountpoint, opts}` names the view by the sender's
   mountpoint (and carries `allow_other`, which a mount other uids use needs), and
   `SessionControl::unmount` on a mount this process did not make *ends* the session —
   stops reading, publishes pending writes, closes every descriptor of the connection —
   instead of refusing; the node plugin unmounts the staging path itself. Tests:
   `crates/frontend-fuse` `unmounting_a_preopened_session_ends_it_without_unmounting`,
   `crates/cli/tests/serve.rs` `a_preopened_view_is_unmounted_by_its_name_without_hanging`
   (both root-only).
4. **A detached descriptor is left `O_NONBLOCK`** (question 1). Harmless for
   `FuseSession::resume`, a livelock for anything else that reads it. **K5** restores
   the blocking mode in the resume path, or `SessionControl::detach` does before
   handing the descriptor out.

## Sources checked out for this plan

| Source | Ref / how checked | Used for |
|---|---|---|
| `container-storage-interface/spec`, `spec.md` (fetched via WebFetch) | tag `v1.13.0` (VERIFIED latest via `gh api repos/container-storage-interface/spec/tags`, 2026-09-28) | Controller/Node/Identity RPC lists, access-mode enum (`SINGLE_NODE_MULTI_WRITER` etc.), `VolumeContentSource`, `DeleteVolume`/`ControllerExpandVolume`/concurrency idempotency clauses |
| `~/.cargo/registry/src/index.crates.io-*/fuser-0.18.0/src/session.rs` (local, VERIFIED read in full for the relevant sections) | fuser 0.18.0, the exact version this workspace already depends on (CONVENTIONS.md: "`fuser` 0.18 with `default-features = false`") | `Session::from_fd` (`:186-207`), `handshake()`'s mandatory-`Init`-first behavior and its `EIO`-then-error failure mode on a non-init first message (`:296-370`, `:342-350`), confirming the design brief's stated handover risk precisely rather than by report |
| `~/.cargo/registry/.../fuser-0.18.0/src/lib.rs` (local) | same | `KernelConfig`'s exact field set (`:209-220`) — the basis for this plan's `NegotiatedInit` field list |
| `~/.cargo/registry/.../fuser-0.18.0/src/mnt/fuse_pure.rs` (local) | same | `fuse_mount_sys`'s own direct `mount(2)` path: opens `/dev/fuse`, builds `"fd={},rootmode={:o},user_id={},group_id={}"` mount options (`:407-412`) — the exact mechanism `platform::linux::fuse_mount_fd` is specified (plan 31) to reuse |
| `kubernetes-csi.github.io/docs/` (WebFetch) | as of 2026-09-28 | standard sidecar list, `CSIDriver` field names |
| `kubernetes-csi.github.io/docs/csi-driver-object.html` (WebFetch) | as of 2026-09-28 | `CSIDriver` field-by-field meanings and defaults (`attachRequired`, `podInfoOnMount`, `fsGroupPolicy` enum + default, `volumeLifecycleModes`, `tokenRequests`, `requiresRepublish`, `seLinuxMount`) |
| `gh api repos/kubernetes-csi/{external-provisioner,external-resizer,external-snapshotter,node-driver-registrar,livenessprobe,csi-test}/releases/latest` | 2026-09-28: `v6.3.0`, `v2.3.0`, `v8.6.0`, `v2.18.0`, `v2.20.0`, `v5.6.0` | Helm chart sidecar image pins |
| `kubernetes-csi/csi-test` README (WebFetch) + `cmd/csi-sanity/main.go` (WebFetch, raw) | `master`, 2026-09-28 | confirms `csi-sanity` is a standalone binary, not just a Go test package; its flag set (`csi.endpoint`, `csi.mountdir`, `csi.stagingdir`, `csi.secrets`, etc.) |
| `kubernetes.io/docs/concepts/storage/volume-pvc-datasource/` (WebFetch) | as of 2026-09-28 | PVC-cloning `dataSource` mechanics and constraints; `ReadWriteOncePod` naming |
| `github.com/awslabs/mountpoint-s3-csi-driver`, `git clone --depth 1` (local, VERIFIED read in full for the cited files, upgraded from an earlier WebFetch-only citation) | commit `b450b22beae8bddc0d3c09551655b2b7d323e29b`, `main`, 2026-09-21, cloned 2026-09-28 into `/tmp/mountpoint-research/mountpoint-s3-csi-driver` | `docs/ARCHITECTURE.md` + `docs/MOUNTPOINT_POD_SHARING.md`: per-(PV, node) "Mountpoint Pod," `MountpointS3PodAttachment` CRD (`pkg/api/v2/mountpoints3podattachment_types.go`'s `{NodeName, VolumeID}` keying) — confirms sharing is scoped per (PV, node), narrower than this plan's per-(pool, node) choice, for a reason (no shared cache to justify coarser sharing, §2.2) rather than a difference Mountpoint merely declined to make; `pkg/driver/node/mounter/pod_mounter.go` + `pkg/mountpoint/mountoptions/mount_options.go`: the exact fd-handoff sequence (open `/dev/fuse` → `mount(2)` → `SCM_RIGHTS` send → close parent's fd copy) this plan's `NodeStageVolume`/`Transport::send_fd` mirrors line-for-line; `mountpoint-s3-fuser/src/session.rs`+`request.rs` (via this driver's vendored fork dependency): `Session::from_fd`'s `initialized: AtomicBool::new(false)` and its exclusive use against freshly-`mount(2)`-ed connections — the basis for §2.2's "no handover precedent exists" finding; `docs/TROUBLESHOOTING.md` + `pkg/podmounter/mppod/creator.go`: `RestartPolicy: OnFailure`, `TerminationGracePeriodSeconds = 600`, SIGTERM-ignoring drain discipline adopted as this plan's always-on baseline (§2.2, §7) |
| `github.com/awslabs/mountpoint-s3`, `git clone --depth 1` (local, VERIFIED) | commit `e144a7bb84948045f0d7cde77060afaa7ed91b53`, `main`, 2026-09-28, cloned into `/tmp/mountpoint-research/mountpoint-s3` | `doc/SEMANTICS.md`: Mountpoint's explicit not-POSIX positioning (§2.2, §"Semantic notes"); `examples/fuse-fd-mount-point/mounthelper.go`: standalone `fd=`/`mount(2)`/`/dev/fd/N` reference confirming `fuse_mount_fd`'s contract independently of the CSI driver |
| This session's own research report, `scratchpad/mountpoint-research.md` (Read in full) | produced this session from the two clones above | consolidated VERIFIED/REPORTED findings and the full source list (§7 of that report) this table's Mountpoint rows summarize; also the source for plan 31's C7/vendored-fuser recommendations this plan cross-references |
| JuiceFS CSI driver smooth-upgrade blog posts (WebSearch, REPORTED — not re-verified against source this session) | as surfaced 2026-09-28 | precedent: FUSE fd passed from Mount Pod to CSI Node over a unix domain socket during "pod recreate" smooth upgrade; Mount Pod sharing across multiple PVs of one filesystem on one node (`FS_SHARE_MOUNT`) — the closer precedent for this plan's per-(pool, node) engine-pod sharing than Mountpoint-S3's per-PV pods, still REPORTED, unlike the now-VERIFIED Mountpoint rows above |
| `kubernetes-sigs/kind` issue #2540 (`gh api`, body + comments) | as of 2026-09-28 | VERIFIED: kind auto-mounts `/dev/fuse` only for rootless docker, not rootful (GitHub-hosted runners' default) — the basis for this plan's explicit `extraMounts` requirement |
| WebSearch results on `/dev/fuse` + Kubernetes device-cgroup behavior (`skypilot-org/skypilot#4108`, `meta-pytorch/monarch#4917`, `pfnet-research/meta-fuse-csi-plugin`) | REPORTED, as surfaced 2026-09-28 | confirms the privileged-pod + device-visible-in-node-container two-layer requirement; `meta-fuse-csi-plugin` is independent precedent for "a privileged CSI-adjacent pod does the mount, hands it to an unprivileged FUSE implementation" matching this plan's node-plugin/engine-pod split |
| WebSearch on `csi-driver-nfs` staging architecture | REPORTED, as surfaced 2026-09-28 | confirms the `NodeStageVolume`-global-mount / `NodePublishVolume`-bind-mount split (settled decision 5) is the standard shared-filesystem CSI pattern, not a novel Constellation invention |
| this tree, `docs/plans/v1/CONVENTIONS.md`, `wip/31-core-frontend-backend.md`, `wip/33-control-plane-and-ui.md`, `wip/35-windows-port.md` (Read, in full or by section) | as of this session, plan 31/33 still in progress in parallel | fixed names, the §9.2 control-method table, the `control-acl.toml` grant format, plan style (options tables, settled-decisions numbering, milestone gate shape, Sources-table format) |
| `docs/plans/v1/done/09-p6a-snapshots-clones.md` (Read, in full) | committed plan | subtree-granular, copy-on-write snapshots/clones (Step 4) — the factual basis for §2.3's "CoW only within one filesystem" and the cross-pool-clone refusal (settled decision 8) |
| `docs/plans/v1/wip/32-snapshot-policies-and-space.md` (Read, relevant sections) | plan 32, in progress in parallel, not edited by this plan | `held_by` owner-namespace field (§0.4) this plan's `CreateSnapshot`/`DeleteSnapshot` set/release; the `_prune`/`_snapsched` `SingletonLease` pattern (§3.2) settled decision 19 explicitly chose *not* to reuse for the purge worker, and why |
| `scratchpad/csi-pool-brief.md` (Read, in full — coordinator's settled volume-pool design) | this session's shared brief, settled, not relitigated | the pool/dedicated layout split, xattr volume records, trash + async purge, shard routing, isolation boundary, and the Mountpoint-lessons summary this plan's §2.2/§2.3/§"Deletion and purge" work in full |
| CSI addendum / core-design-brief scratchpad files (Read) | this session's shared brief | every fixed name and architectural decision this plan is required to use verbatim |
