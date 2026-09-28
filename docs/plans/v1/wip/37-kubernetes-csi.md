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
  control methods this plan drives (`fs.unlock`, `view.mount`, `view.unmount`,
  `view.list{labels}`, `view.stats`, `quota.set`, `quota.get`,
  `snapshot.create{hold}`, `snapshot.delete`, `clone.create`, `node.leave`,
  `node.handoff`), `EphemeralSecretStore` and per-engine `CredentialSource`.
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
above is quoted verbatim from the shared design brief; this plan does not
rename any of them.

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

The one genuinely hard new problem this plan introduces is **FUSE-in-Kubernetes
churn**: a kernel FUSE mount is pinned to the process that opened
`/dev/fuse`. Every existing FUSE-backed CSI driver either (a) runs the FUSE
process inside the *node plugin* container, which means a driver
upgrade/crash/OOM kills every mount on that node (`ENOTCONN`, "Transport
endpoint is not connected", surfaced to every pod using the filesystem), or
(b) accepts that cost as a known limitation. Constellation's engine already
separates cleanly from its FUSE adapter (plan 31 C4's `Vfs` contract) and
already treats FUSE session lifetime as something the engine, not the
frontend, should own (`kernel_inval.rs`'s notifier thread, the responder
completion pool) — so this plan can do better: put the FUSE-holding process
in its own long-lived pod, decoupled from the CSI node plugin's own
lifecycle, and go one step further with **session handover** so that even
*that* pod's own upgrades don't interrupt writers. This mirrors precedent
(§14(d)) from two production FUSE CSI drivers that independently arrived at
"FUSE process in its own pod, fd handed over the unix socket" — Mountpoint
for Amazon S3 CSI driver v2 and the JuiceFS CSI driver's mount-pod mode —
and pushes one step further than both by making the *handover itself*
lossless for open file handles, not just for the mount point.

## 2. Decision: where does the FUSE session live?

| Option | Node-plugin crash blast radius | Engine-pod upgrade blast radius | Shares engine/cache across PVs of one fs | Extra moving parts | Verdict |
|---|---|---|---|---|---|
| **FUSE in the node plugin container itself** (classic pattern: most first-generation FUSE CSI drivers, e.g. early Mountpoint-S3 CSI v1, most `goofys`/`s3fs` sidecar drivers) | Every mount on the node dies (`ENOTCONN`) on every node-plugin restart, including routine driver upgrades and `livenessprobe`-triggered restarts | n/a (same process) | No — one process per node total, no per-fs isolation, one noisy-neighbour PV can starve every other PV's cache | Fewest — single privileged DaemonSet pod | **Rejected.** Kubernetes restarts DaemonSet pods routinely (upgrades, node pressure, `livenessprobe`); making every restart a filesystem outage for every pod on the node is not acceptable for what is supposed to be Constellation's *strongest* multi-writer story. |
| **One engine (and FUSE session) per PV** | A crash only affects that PV's pods | An upgrade of one PV's engine doesn't affect others, but N PVs on a node means N processes, N caches, N sets of leases/P2P listeners for what might be the *same* filesystem | No — defeats the purpose of a shared working-set cache across PVs of one filesystem on one node | Most — the pod count scales with PV count, not node count or filesystem count | **Rejected.** Constellation's value is the shared cache and lease/coop machinery; splitting it per PV means two pods on the same node mounting the same filesystem's two different subtrees can't share a cache page, can't share a lease, and pay S3/P2P costs twice. It also multiplies `EngineProfile` memory/cache budgets by PV count instead of by filesystem count. |
| **One engine pod per (Constellation filesystem, k8s node), FUSE fd handed off from the node plugin** — **chosen** | A crash affects only PVs of that one filesystem on that one node; other filesystems and other nodes are unaffected | An upgrade of one engine pod affects only that filesystem's PVs on that node, and with session handover (§9) not even that, for in-flight I/O | Yes — `View`s for every PV of that filesystem on that node share one `Engine`'s cache, leases and P2P endpoint | Moderate — pod count scales with distinct (filesystem, node) pairs actually in use, which is the natural unit of resource sharing | **Chosen.** Matches the natural sharing boundary (one filesystem = one working set, one lease namespace, one P2P identity per node) while keeping blast radius at exactly that boundary, and lets the *unprivileged* engine pod do all the engine work while only the *privileged* node plugin ever calls `mount(2)`. |
| **Kernel NFS re-export**: a Constellation-owned NFS server (plan 34's `frontend-nfs`, in-cluster) fronting the engine, mounted by kubelet via the built-in Linux NFS client, no FUSE in the picture at all | A server pod restart is a normal NFSv4.1 reconnect (grace period), not `ENOTCONN` — genuinely more resilient to restarts than raw FUSE | Same resilience story as the crash case — NFSv4.1 clients recover across a server restart within the grace period | Yes, same sharing boundary as the chosen option if scoped per filesystem | Fewer new primitives *in Kubernetes* (no fd passing, no session handover, no privileged node plugin beyond the kernel NFS client's own mount call) but a large new one *in Constellation* — plan 34's NFSv4.1 server has to exist and be hardened first | **Rejected for this plan, recorded as the honest runner-up.** It trades this plan's hardest problem (FUSE session handover) for a dependency this plan cannot assume: plan 34's NFS frontend, built and hardened for a *desktop* macOS client, would need to become a multi-tenant, quota-enforcing, label-aware server fronting arbitrary numbers of Kubernetes-mounted filesystems — materially more surface than "expose one already-open FUSE fd to a sibling pod." It also reintroduces exactly the network round-trip on every read that FUSE-local-then-S3-cold-then-P2P avoids: an in-cluster NFS re-export sits *between* the pod and the engine's cache, whereas the FUSE-fd-in-engine-pod design puts the engine's cache in the same failure/latency domain as the FUSE mount, one hop closer to the workload. If K0's fd-passing/handover spike turns out to be infeasible even after the pre-agreed fallbacks (§13's K0 gate), this is the documented plan B, not a stopgap improvised under pressure. |

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
   `globalmount` staging layout — precedent §14(h)). `NodePublishVolume` is
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
7. **PV = View, subtree = `/<pv-name>` at the StorageClass's filesystem
   root.** `CreateVolume`'s `req.Name` (the deterministic name
   `external-provisioner` derives from the PVC, `pvc-<uid>`) is used
   verbatim as both the subtree directory name and the CSI `volume_id` — no
   separate ID mapping table, which also makes every controller RPC trivially
   idempotent on `req.Name`/`volume_id` (§"CSI RPC mapping" column 3).
8. **Snapshots and clones use plan 32's holds; this plan does not implement
   its own snapshot storage.** `CreateSnapshot` = `snapshot.create{hold:
   "csi:<VolumeSnapshotContent uid>"}`; a volume created `FromSnapshot` or
   `FromVolume` (PVC clone) = `clone.create`. See §"Snapshots and clones".
9. **Credentials arrive per-request, never baked into the engine-pod image
   or a long-lived Kubernetes `Secret` mount the engine pod reads for
   itself.** `StorageClass`/`VolumeSnapshotClass` reference a k8s `Secret`
   by the standard `csi.storage.k8s.io/*-secret-name`/`-namespace`
   parameters; the CSI sidecars resolve it and hand it to
   `constellation-csi` as request bytes; `constellation-csi` forwards it to
   the engine pod as `fs.unlock`/`view.mount` params, held only in that
   engine pod's `EphemeralSecretStore` (memory-only, VERIFIED named in the
   design brief's required plan-31 seam list).
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
12. **`podInfoOnMount: true`, `requiresRepublish: false`.** The driver wants
    `csi.storage.k8s.io/pod.name`/`.namespace`/`.uid`/`.serviceAccount.name`
    in `NodePublishVolume`'s `volume_context` purely for the `view.mount`
    labels (§"Observability") — it does not need periodic republish, since
    engine-pod session handover (§9) is how mount state survives churn, not
    republish-driven remounting.
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
                                  │ fs.registry.*, quota.*, snapshot.*, clone.*
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
    `DeleteSnapshot`, `ListSnapshots`, `ValidateVolumeCapabilities`. Talks to
    *any* reachable engine pod for the target filesystem (controller RPCs
    are node-independent registry/quota/snapshot operations — they don't
    need the specific node's engine pod, just *an* engine pod, or a small
    always-on "registry" control connection; see §"Engine-pod lifecycle" for
    how the controller reaches one without requiring a PV to already be
    mounted anywhere).
  - **Node service**: implements `NodeGetCapabilities`, `NodeGetInfo`,
    `NodeStageVolume`, `NodeUnstageVolume`, `NodePublishVolume`,
    `NodeUnpublishVolume`, `NodeGetVolumeStats`, `NodeExpandVolume` (no-op,
    settled decision 6). Owns the privileged `fuse_mount_fd` call and the
    engine-pod lifecycle for its own node (§"Engine-pod lifecycle").
- **Engine pods**, one per (Constellation filesystem, k8s node) actually in
  use, named `constellation-engine-<fs>-<node>`, running the `constellation`
  binary in daemon mode as an `EngineHost` with exactly one `Engine`. Hosts
  every `View` (= PV) of that filesystem mounted on that node, sharing the
  `Engine`'s meta store, cache, lease/P2P identity and `ResourceBudget`.
  Unprivileged: no `mount(2)`, no `CAP_SYS_ADMIN`; it only ever receives
  already-open fds over its control socket and serves them with
  `MountSource::PreopenedFd`.
- **hostPath layout** (per node, see §"Engine-pod lifecycle" for the full
  tree) carries the fd-passing unix sockets and engine-pod control sockets;
  it is how the privileged node plugin and the unprivileged engine pods
  rendezvous without a network hop.

Control-protocol methods this plan drives, all from the plan-31 §9.2 table
(cited by name, not renumbered here):

| Method | Called by | When |
|---|---|---|
| `fs.registry.list`/`fs.registry.create` | controller | `CreateVolume` on first use of a filesystem (imports/creates the registry entry if the StorageClass names one that doesn't exist yet, gated by an explicit StorageClass parameter — see §"StorageClass parameters") |
| `fs.unlock` | node plugin → engine pod | `NodeStageVolume`, supplying S3 + E2E credentials before the first `view.mount` on a freshly (re)started engine pod |
| `view.mount` | node plugin → engine pod | `NodeStageVolume`, with `source: PreopenedFd`, subtree `/<pv-name>`, `labels: {pv, pvc, namespace}` |
| `view.unmount` | node plugin → engine pod | `NodeUnstageVolume` |
| `view.list{labels}` | controller (health/GC), node plugin (idempotency checks) | periodic reconciliation, `NodeStageVolume` retries |
| `view.stats` | node plugin | `NodeGetVolumeStats` |
| `quota.set` | controller | `CreateVolume` (initial size), `ControllerExpandVolume` |
| `quota.get` | controller | `NodeGetVolumeStats` capacity fields, `ValidateVolumeCapabilities` |
| `snapshot.create{hold}` | controller | `CreateSnapshot` |
| `snapshot.delete` | controller | `DeleteSnapshot` (releases the hold first) |
| `clone.create` | controller | `CreateVolume` with a `VolumeContentSource` |
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
| `Controller.CreateVolume` | `fs.registry.create` (if StorageClass says to auto-create; §"StorageClass parameters") once, then `quota.set{subtree:/<name>, bytes: req.capacity_range.required_bytes}` — the subtree itself is created implicitly by the first `quota.set`/`view.mount`, matching Constellation's existing "a View's root need not pre-exist" behavior | `req.name` (`pvc-<uid>`, provisioner-supplied, stable across retries) | `Code::Exists` with different params → `ALREADY_EXISTS`; quota exceeding the filesystem's own hard cap → `RESOURCE_EXHAUSTED`; unknown filesystem name → `NOT_FOUND`; bad StorageClass parameters → `INVALID_ARGUMENT`; concurrent create for the same name already in flight → `ABORTED` (in-process per-`req.name` mutex, §"Concurrency guard" below) |
| `Controller.DeleteVolume` | `quota.set{subtree:/<name>, bytes: 0}` then subtree delete (a `browse.delete` equivalent scoped to the engine's own registry-owned root — no kernel mount involved) | `req.volume_id` | already gone → `OK` (CSI requires delete to be idempotent-safe on a missing volume, VERIFIED spec.md `DeleteVolume`: "This operation MUST be idempotent... MUST return `OK`... if the volume does not exist"); in-flight `snapshot.create` referencing it → `FAILED_PRECONDITION` |
| `Controller.ControllerExpandVolume` | `quota.set{subtree:/<name>, bytes: req.capacity_range.required_bytes}` | `req.volume_id` (quota.set is naturally idempotent — setting the same value twice is a no-op) | new size below `used_bytes` → `OUT_OF_RANGE` (VERIFIED spec.md: `ControllerExpandVolume` "grow only", shrink unsupported) |
| `Controller.CreateSnapshot` | `snapshot.create{hold: "csi:<content-uid>", selector: subtree /<source-volume-id>}` | `req.name` | source volume busy with an incompatible op → `ABORTED`; a snapshot already exists under a *different* hold for the same `req.name` → `ALREADY_EXISTS` |
| `Controller.DeleteSnapshot` | release the `csi:<content-uid>` hold, then `snapshot.delete` if no other hold remains | `req.snapshot_id` | missing → `OK` (same idempotency rule as `DeleteVolume`) |
| `Controller.ListSnapshots` | `snapshot.list` filtered to holds prefixed `csi:` | n/a (read-only) | pagination token mismatch → `ABORTED` per spec's `starting_token` contract |
| `Controller.ValidateVolumeCapabilities` | `quota.get` (existence check) plus a static compatibility check against settled decision 14's advertised modes | `req.volume_id` | volume missing → `NOT_FOUND` |
| `Controller.ControllerGetCapabilities` | static | n/a | n/a |
| `Node.NodeStageVolume` | ensure the (fs, node) engine pod exists and is `Ready` (§"Engine-pod lifecycle"); `fs.unlock` if this is the pod's first volume; `fuse_mount_fd(globalmount_path, opts)`; `Transport::send_fd` the resulting `OwnedFd` to the engine pod; `view.mount{source: PreopenedFd, subtree: /<pv-name>, labels}` | `req.volume_id` + `req.staging_target_path` (re-staging the same path when already staged is a no-op, VERIFIED spec.md `NodeStageVolume` idempotency clause) | engine pod unreachable after the timeout → `DEADLINE_EXCEEDED`; wrong fs credentials → `PERMISSION_DENIED`; already staged at a *different* path → `ALREADY_EXISTS` |
| `Node.NodeUnstageVolume` | `view.unmount`; if this was the engine pod's last `View`, mark it idle (GC'd after a grace period, §"Engine-pod lifecycle") | `req.volume_id` | not staged → `OK` (idempotent) |
| `Node.NodePublishVolume` | bind mount (`MS_BIND`[`|MS_RDONLY` for ROX]) from the staging path to `req.target_path` | `req.volume_id` + `req.target_path` | staging path missing (staged elsewhere/never) → `FAILED_PRECONDITION`; already published at this exact path → `OK` |
| `Node.NodeUnpublishVolume` | unmount the bind mount at `req.target_path` | `req.volume_id` + `req.target_path` | not mounted there → `OK` |
| `Node.NodeGetVolumeStats` | `view.stats` (statfs-shaped: `rsize`/`rcount`, plus `quota.get` for capacity) | n/a (read-only) | volume path not a mount → `NOT_FOUND` |
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

```yaml-k8s
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: constellation-rwx
provisioner: csi.constellation.dev
parameters:
  # Which Constellation filesystem (registry name) this class provisions
  # PVs into. Required.
  filesystem: "team-shared"
  # If "true", CreateVolume calls fs.registry.create for `filesystem` on
  # first use instead of failing NOT_FOUND. Defaults to "false" — most
  # clusters want the filesystem created once, deliberately, out of band
  # (`constellation fs create`), not implicitly from a StorageClass.
  autoCreateFilesystem: "false"
  # EngineProfile preset used when constellation-csi brings up a new
  # engine pod for this filesystem. "server" (plan 31 C8's dense
  # multi-PV preset) is the default and the only supported value at K0-K6;
  # a future per-StorageClass override is deferred (Risks).
  engineProfile: "server"
  # csi.storage.k8s.io/*-secret-name / -namespace are the standard CSI
  # secret-reference parameters (VERIFIED external-provisioner/
  # external-resizer honor this exact key convention): resolved by the
  # sidecars, delivered to constellation-csi as request bytes, forwarded
  # to the engine pod's EphemeralSecretStore. Never written to a
  # ConfigMap, never logged.
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
  # Constellation E2E passphrase, if the filesystem is E2E-encrypted.
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

## 7. Engine-pod lifecycle

**hostPath layout, per node** (root `/var/lib/constellation-csi/`, created
by the node plugin's `DaemonSet` with a `hostPath` volume of type
`DirectoryOrCreate`):

```
/var/lib/constellation-csi/
├── node-identity/<fs>/                # Constellation node key + roster
│                                       # state for this (fs, node) pair —
│                                       # survives engine-pod restarts,
│                                       # deleted only by node.leave's
│                                       # cleanup or explicit node removal.
├── sockets/<fs>/
│   ├── fd-handoff.sock                # node plugin ↔ engine pod, fd passing
│   └── control.sock                   # engine pod's constellation-control
│                                       # UnixSocket server (operator role
│                                       # for the node plugin's uid; see
│                                       # "Credentials and security")
└── staging/<pv-name>/globalmount/     # NodeStageVolume mountpoints (bind
                                        # source for every pod's publish)
```

**Creation.** `NodeStageVolume` for the first PV of a filesystem on a node:
the node plugin checks for a `Ready` `constellation-engine-<fs>-<node>` pod
via the Kubernetes API (list by label `constellation.dev/fs=<fs>,
constellation.dev/node=<node>`); if absent, it creates one — a bare `Pod`
object (not a `Deployment`/`StatefulSet`: its identity is entirely
determined by (fs, node) and it is never rescheduled, only replaced in place
by the node plugin itself, which already has to orchestrate replacement for
session handover, §9) with:

- `nodeName: <node>` (no scheduler round-trip — the node plugin already
  knows exactly which node it's on);
- `hostPath` mounts for `node-identity/<fs>/` and `sockets/<fs>/` (`Bidirectional`
  mount propagation is **not** needed here — only the node plugin's own
  `mount(2)` calls need propagation *out* to the host and kubelet's view;
  the engine pod never calls `mount(2)` at all, settled decision 10);
- `securityContext: { runAsNonRoot: true, allowPrivilegeEscalation: false,
  capabilities: { drop: ["ALL"] }, seccompProfile: { type: RuntimeDefault }
  }` — no privileges beyond opening a unix socket and receiving an
  already-open fd over it, which needs none;
- resource `requests`/`limits` from the StorageClass's `engineProfile`
  parameter, translated into the container's memory limit plus the
  `EngineProfile.memory_budget`/`cache_budget` fields passed as daemon flags
  — the container limit is set slightly above the engine's own budget so
  the engine's internal admission control (plan 31 §6.3's backpressure
  barrier) is what sheds load, not the OOM killer;
- an `emptyDir` (`medium: Memory`, sized from `cache_budget`) for the
  engine's local cache, so cache eviction on pod replacement is free and
  the node's real disk isn't consumed by cache that a restart discards
  anyway (durable state — the meta store, staging writes not yet
  acknowledged to S3 — lives under `node-identity/<fs>/`, on the real
  hostPath, and survives).

**Readiness.** The pod's own `constellation` daemon exposes a startup/liveness
probe hitting its control socket's `node.ping`; the node plugin polls
Kubernetes `Pod.status.phase == Running` plus that probe before proceeding
with `fs.unlock`/`view.mount`.

**Ownership and GC.** Each engine pod carries an `ownerReference` to the
node plugin's own `DaemonSet` Pod (so it cannot outlive the node plugin
generation that created it — a stale engine pod from a deleted node plugin
gets garbage-collected by Kubernetes itself) plus a `constellation.dev/
last-view-count` annotation the node plugin updates on every
`view.mount`/`view.unmount`. A background reconcile loop in the node plugin
(polling every 30s) deletes any engine pod whose `view.list` returns zero
views and whose `last-view-count` has been zero for longer than a
`--engine-pod-idle-ttl` flag (default 10 minutes — long enough to absorb a
PVC being briefly unmounted/remounted across a pod restart without
thrashing engine pods, short enough not to waste memory on genuinely
abandoned filesystems).

**Node identity.** Constellation's per-node roster identity (the node key,
whatever `authority`/`registry` state a live node needs to rejoin its peer
group) is keyed by (filesystem, k8s node), not by pod — it lives on the
`node-identity/<fs>/` hostPath directory precisely so that replacing the
engine pod (upgrade, crash, OOM) does **not** look like a node leaving and
rejoining the cluster's roster; it is the same Constellation node
re-attaching, which is what makes §9's session handover meaningful (a
`node.handoff` between two engine-pod *processes* sharing one node identity,
not two different Constellation nodes).

**Drain.** The node plugin registers a `preStop` hook (and separately,
watches for `Node.Unschedulable`/`kubectl drain` via a `PodDisruptionBudget`-aware
controller loop) that, before a node is fully drained, calls control
`node.leave{force: false}` for every engine pod on that node — releasing
leases cleanly and removing the node from the filesystem's roster instead of
leaving a ghost entry that `docs/plans/v1`'s existing roster-fairness work
(commit `27941de`, "the lock queue is served in arrival order") would
otherwise have to time out. Autoscaled node churn (cluster-autoscaler scaling
a node pool down) goes through the same drain path — this is explicitly
flagged as a correctness requirement, not an optimization: an autoscaler that
kills nodes without a drain hook would bloat the roster with dead entries on
every scale-down, exactly the failure mode the design brief calls out
("Autoscaled churn must not bloat the roster").

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
image tag and performs the rollout, one (fs, node) pod at a time), or an
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
   (fs, node) pair — a whole-pod handoff, not per-PV, since they share one
   FUSE fd... **wait, they do not**: settled decision 5 already established
   each PV gets its *own* `fuse_mount_fd` call at `NodeStageVolume` time, so
   each `View` has its own FUSE session and its own fd. §9's handover is
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
   fd-bearing frame.
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
  (fs, node) — allowed, since PV≠fd sharing already means each `View` is
  independent) and the failure is surfaced as a `node.handoff` partial-result
  response the node plugin logs and alerts on, never silently swallowed.
- Step 5 fails for *every* `View`, or the vendored resume patch itself is
  unavailable/broken in a given build → the **pre-agreed fallback from the
  design brief** applies, and is a first-class, documented mode, not an
  emergency patch: *"keep the old engine pod alive until PVs republish via
  requiresRepublish."* Concretely: `requiresRepublish: true` becomes a
  per-engine-pod runtime toggle (not the static CSIDriver-wide `false` from
  settled decision 12) the node plugin flips on for a filesystem whose
  handover keeps failing, causing kubelet to periodically re-call
  `NodePublishVolume`, which the node plugin uses as its cue to retry the
  bind-mount against a freshly staged new engine pod the *old-fashioned*
  way (full unmount/remount, accepting the `ENOTCONN` window Kubernetes
  users of every other FUSE CSI driver already live with) — strictly worse
  than a lossless handover, but never worse than the pre-plan-37 status quo,
  and it means a K0 finding of "handover is infeasible on some kernel/fuser
  combination" degrades this plan's value, it does not block shipping it.
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
  of §9's `node.handoff` `Prepare` phase against the *new* pod before any
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

  `operator` (not `admin`) is sufficient for every method in
  §"Control-protocol methods" — none of them are roster/security-admin
  operations reserved to `admin` (allowlist edits, audit-log reads). Every
  engine pod's `control.sock` carries this exact grant; the controller's own
  connections (for `fs.registry.*`, `snapshot.*`, `quota.*` against whichever
  engine pod it reaches) use the *same* service-principal identity —
  `constellation-csi` the controller binary and `constellation-csi` the node
  binary are, deliberately, one authorization identity, since splitting them
  would only add configuration surface without a real isolation boundary
  (both already have to be trusted with full filesystem read/write to do
  their jobs).
- **RBAC (Kubernetes).** Standard CSI sidecar `ClusterRole`s
  (`external-provisioner`, `external-resizer`, `external-snapshotter`
  upstream-recommended rules, unmodified) plus a `constellation-csi`-specific
  `ClusterRole` scoped to: `pods` (get/list/watch/create/delete, for engine-pod
  lifecycle, namespace-scoped to wherever the driver itself is installed —
  never cluster-wide pod creation), `persistentvolumeclaims`/`persistentvolumes`
  (get, for the `pv`/`pvc`/`namespace` labels in §"Observability"), and
  `secrets` (get, scoped by the standard CSI secret-reference convention, not
  a blanket cluster-wide secrets read).
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
- **Which pods are privileged, summarized**: node plugin — yes
  (`CAP_SYS_ADMIN` via `privileged: true`, `Bidirectional` mount
  propagation). Controller — no. Engine pods — no. This 1-privileged-role
  design is the direct payoff of settled decision 10 and the reason the
  fd-passing architecture exists at all: it is strictly fewer privileged
  processes than "FUSE in the node plugin" (still one) but with a *much*
  smaller blast radius per restart, and strictly fewer than "engine mounts
  its own fd" (which would require every engine pod to be privileged).

## 10. Observability

- **View labels.** Every `view.mount` call sets `ViewSpec.labels =
  {"pv": req.volume_id, "pvc": <from volume_context, podInfoOnMount>,
  "namespace": <from volume_context>}` (settled decision 12's reason for
  wanting `podInfoOnMount` — `pvc`/`namespace` aren't otherwise visible to
  the node plugin from the CSI request alone in every code path, only
  `pod.*` fields are guaranteed present; `pvc`/`namespace` come from
  `external-provisioner`'s own convention of also injecting
  `csi.storage.k8s.io/pvc/name` and `.../pvc/namespace` into
  `volume_context` when `--extra-create-metadata` is set on the
  provisioner sidecar — this plan's Helm chart sets that flag by default).
- **Metrics.** `constellation_vfs_ops_total{frontend="fuse",op,outcome}` and
  `constellation_vfs_op_seconds{frontend="fuse",op}` (plan 31 §6.10) carry
  the bounded label allowlist `{view, pv}` — never raw `pvc`/`namespace` on
  the high-cardinality per-op histograms, to keep Prometheus cardinality
  bounded by PV count, not by (PV × pod × namespace) count; `pvc`/`namespace`
  remain queryable via `view.list{labels}` for a control-plane-level join
  (plan 33's UI does exactly this for its screen 3). Engine-pod-level
  metrics (`EngineHost` resource-budget gauges, fd-handoff duration
  histogram from §9's step 6 measurement) are additionally labeled
  `{fs, node}`.
- **Events on PVCs.** `constellation-csi` emits Kubernetes `Event`s on the
  `PersistentVolumeClaim` object (via the standard `client-go`
  `EventRecorder`, the same mechanism `external-provisioner` already uses
  for `ProvisioningSucceeded`/`ProvisioningFailed`) for: engine-pod created/
  ready/replaced (handover), quota near/at limit (a `quota.get` threshold
  check on `NodeGetVolumeStats`, mirroring plan 33's own "quota thresholds"
  notification but surfaced where a Kubernetes operator actually looks —
  `kubectl describe pvc`), and session-handover fallback engaged (§9's
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
  Constellation's `quota.set`/`quota.get` — a *soft* accounting limit the
  engine enforces at write time (`ENOSPC` once the quota is hit), not a
  pre-allocated block range the way an EBS volume's size is. This means
  `GetCapacity` (the optional Controller RPC reporting *available raw
  storage* for the whole backing pool) is meaningless for an S3-backed
  filesystem in the way it is for a fixed-size block pool — this plan does
  not implement it (not advertised in `ControllerGetCapabilities`), which is
  a correct, deliberate omission, not an oversight: S3's advertised
  capacity is not a number Constellation should be putting in front of the
  Kubernetes scheduler's capacity-aware provisioning logic (`storageCapacity`
  in `CSIDriver` is likewise left unset/`false`).
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
`upgrade-under-load` is the direct CI expression of §9's headline guarantee
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
    resource requests/limits for every container, and the PodSecurity
    posture note from §"Credentials and security" spelled out in a
    chart-level `NOTES.txt` warning about the node-plugin namespace's
    `privileged` requirement.
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

### K0 — Spike: fd-passing mount + session handover prototype (timebox: 5 days)

**What to build.** A throwaway example outside the product, exercising the
full chain end to end without any Kubernetes involved yet:
`crates/frontend-fuse/examples/handover_probe.rs` (or an equivalent
temporary binary): mount via `fuse_mount_fd` at a scratch path, pass the fd
to a second process over a unix socket (`SCM_RIGHTS`), have the second
process wrap it and serve a trivial in-memory tree, then — the actual
spike — hand it off to a *third* process using the vendored
`FuseSession::resume` patch, with a `dd`/`fio` write loop running against
the mountpoint throughout, asserting zero I/O errors observed by the writer
across the handoff.

**Questions:**

1. Does the `fuser` `Session::from_fd` handshake failure mode predicted in
   §9's step 5 (EIO reply to a real request, then a hard error) actually
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
   estimated — this is the number §9's "under 2 seconds" target is checked
   against.
4. Do in-flight requests genuinely survive the pause (step 1-2 of §9) with
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

**Pre-agreed consequences** (from the design brief, restated precisely):

- **Handover (questions 1-2) works as designed:** proceed to K1-K7 as
  planned; the vendored patch is upstreamed as a plan-31 C4 deliverable
  (`vendor/fuser`).
- **Handover is infeasible even after reasonable patch iteration** (e.g. the
  kernel's FUSE connection state has some other process-identity coupling
  beyond `INIT` that this session's `session.rs` reading didn't surface):
  fall back to the brief's pre-agreed degraded mode — document
  restart-with-remount, accept the `ENOTCONN` window, **or** keep the old
  engine pod alive until PVs republish via `requiresRepublish` (§9's
  "Failure handling" section already specifies this as a *runtime* fallback
  for individual failed handoffs; if K0 shows it is the *only* available
  mode, it becomes the default and only mode, and §9's protocol steps 1-3
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

**Gate:** the questions above answered and recorded in §"K0 results", with
the consequence taken for each explicitly stated.

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

- `CreateVolume`, `DeleteVolume`, `ControllerExpandVolume`,
  `ControllerGetCapabilities`, `ValidateVolumeCapabilities` against
  `fs.registry.*`/`quota.*`.
- `external-provisioner` + `external-resizer` sidecars wired into the Helm
  chart; a `StorageClass` provisions a bound `PersistentVolume` end to end
  (no mounting yet — `NodeStageVolume` still stubbed).
- **Gate:** CONVENTIONS gates; `csi-sanity`'s Controller test group passes
  (against the in-memory backend, per §"Testing"); `kubectl apply` a
  `StorageClass`+`PVC` reaches `Bound`.

### K3 — Node service: stage, publish, mount, RWX

- `NodeStageVolume`/`NodeUnstageVolume` (real `fuse_mount_fd` +
  `view.mount{PreopenedFd}`, on-demand engine-pod creation per
  §"Engine-pod lifecycle", minus GC/drain), `NodePublishVolume`/
  `NodeUnpublishVolume` (bind mount), `NodeGetVolumeStats`.
- `kind-e2e` CI job stood up (§"CI"), including the `extraMounts` +
  privileged node-plugin config validated in K0.
- **Gate:** CONVENTIONS gates; `csi-sanity`'s Node test group passes against
  a real kind cluster (not the in-memory K1/K2 backend); a pod mounts a PV
  and reads/writes through it; the RWX-across-nodes `k8s-scenario` passes.

### K4 — Snapshots and clones

- `CreateSnapshot`/`DeleteSnapshot`/`ListSnapshots` against
  `snapshot.create{hold}`/`snapshot.delete` (depends on plan 32's
  `held_by` owner namespaces and REFER accounting — §16; K4 starts only
  after plan 32 is committed, with no ad hoc hold-naming scheme in the
  meantime).
  `CreateVolume`-from-`VolumeContentSource` (clone/restore) against
  `clone.create`.
  `external-snapshotter` sidecar + `snapshot-controller`/CRDs added to the
  chart's prerequisites.
- **Gate:** CONVENTIONS gates; the snapshot→clone→mount `k8s-scenario`
  passes; `ListSnapshots` pagination matches `csi-sanity`'s expectations.

### K5 — FUSE session handover in production

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
  under the §9 "under 2 seconds" target at p99.

### K6 — Credentials, security, drain, GC

- `EphemeralSecretStore` wiring for all three `CredentialSource` variants,
  secret-rotation scenario, the `control-acl.toml` service-principal grant,
  PodSecurity posture finalized, engine-pod idle-GC and node-drain
  (`node.leave`) implemented per §"Engine-pod lifecycle".
- **Gate:** CONVENTIONS gates; the secret-rotation and node-drain
  `k8s-scenario`s pass; a `kube-bench`-style PodSecurity check confirms
  the node-plugin/engine-pod privilege split matches
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
  concrete recommendation.
- **Gate:** every CI job in §"CI" green; the full `k8s-scenario` suite
  green; `PROGRESS.md`/`TESTING.md` updated; the how-to guide for deploying
  Constellation as a Kubernetes CSI driver exists.

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
  `pv`/`pvc`/`namespace` labels. Screen 5 marks `csi:`-held snapshots as
  externally owned; deleting one needs admin and an explicit override.

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
   results matrix; `TESTING.md` covers the `linux-csi` lane, `csi-sanity`
   invocation and the `k8s-scenario` harness mode; a how-to guide for
   deploying the Helm chart exists; the Plan 32/33 change requests
   (§"Plan 32 and 33 changes needed") are filed as tracked follow-ups, not
   silently left implicit.
7. **Report**: per-job CI pass/fail tallies, the K0 results matrix, the K5
   handoff-duration p50/p99 and the 20-run `ENOTCONN` tally, and the parity
   summary.

## K0 results

*(filled in by the executing model)*

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
| `awslabs/mountpoint-s3-csi-driver`, `docs/ARCHITECTURE.md` + `docs/MOUNTPOINT_POD_SHARING.md` (WebFetch, raw) | `main`, 2026-09-28 | precedent (d): per-volume "Mountpoint Pod," `MountpointS3PodAttachment` CRD, the exact fd-handoff sequence (open `/dev/fuse` → `mount(2)` → send fd + mount options over a unix socket) this plan's `NodeStageVolume` design mirrors; confirms sharing is scoped per (PV, node), a deliberate point of comparison against this plan's per-(fs, node) sharing choice |
| JuiceFS CSI driver smooth-upgrade blog posts (WebSearch, REPORTED — not fetched from primary source docs directly) | as surfaced 2026-09-28 | precedent (d): FUSE fd passed from Mount Pod to CSI Node over a unix domain socket during "pod recreate" smooth upgrade; Mount Pod sharing across multiple PVs of one filesystem on one node (`FS_SHARE_MOUNT`) — the closer precedent for this plan's per-(fs, node) engine-pod sharing than Mountpoint-S3's per-PV pods |
| `kubernetes-sigs/kind` issue #2540 (`gh api`, body + comments) | as of 2026-09-28 | VERIFIED: kind auto-mounts `/dev/fuse` only for rootless docker, not rootful (GitHub-hosted runners' default) — the basis for this plan's explicit `extraMounts` requirement |
| WebSearch results on `/dev/fuse` + Kubernetes device-cgroup behavior (`skypilot-org/skypilot#4108`, `meta-pytorch/monarch#4917`, `pfnet-research/meta-fuse-csi-plugin`) | REPORTED, as surfaced 2026-09-28 | confirms the privileged-pod + device-visible-in-node-container two-layer requirement; `meta-fuse-csi-plugin` is independent precedent for "a privileged CSI-adjacent pod does the mount, hands it to an unprivileged FUSE implementation" matching this plan's node-plugin/engine-pod split |
| WebSearch on `csi-driver-nfs` staging architecture | REPORTED, as surfaced 2026-09-28 | confirms the `NodeStageVolume`-global-mount / `NodePublishVolume`-bind-mount split (settled decision 5) is the standard shared-filesystem CSI pattern, not a novel Constellation invention |
| this tree, `docs/plans/v1/CONVENTIONS.md`, `wip/31-core-frontend-backend.md`, `wip/33-control-plane-and-ui.md`, `wip/35-windows-port.md` (Read, in full or by section) | as of this session, plan 31/33 still in progress in parallel | fixed names, the §9.2 control-method table, the `control-acl.toml` grant format, plan style (options tables, settled-decisions numbering, milestone gate shape, Sources-table format) |
| CSI addendum / core-design-brief scratchpad files (Read) | this session's shared brief | every fixed name and architectural decision this plan is required to use verbatim |
