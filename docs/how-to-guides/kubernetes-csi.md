# Deploy Constellation as a Kubernetes CSI driver

Give Kubernetes workloads `PersistentVolume`s backed by Constellation on S3:
install the `constellation-csi` Helm chart, pick a layout per
`StorageClass`, and provision volumes from it. The driver is
`csi.constellation.dev`: a controller `Deployment` (provisioning,
expansion, snapshots, trash purge) and a node plugin `DaemonSet` (the
FUSE mounts). Both start **engine pods**, one `constellation serve`
process per pool filesystem per node, which serve every volume of that
pool on that node. The design is plan 37
(`docs/plans/v1/done/37-kubernetes-csi.md`). The chart's own
reference is `deploy/helm/constellation-csi/README.md` (privilege split,
credentials, control-socket identity).

## Before you start

- Kubernetes **1.30 or later** (the chart's ValidatingAdmissionPolicies;
  `controller.podAccessPolicy=false` and `node.podAccessPolicy=false` for
  older clusters, which makes the plugins' ServiceAccounts root-equivalent).
- Nodes with `/dev/fuse` and a container runtime that lets a privileged pod
  open it. Only the node plugin is privileged. Engine pods and the
  controller are not.
- An S3 bucket (or an S3-compatible endpoint) and credentials for it.
  Constellation does not create buckets.
- For snapshots and clones: the snapshot CRDs and the cluster-wide
  `snapshot-controller` (external-snapshotter v8.6.0), installed once per
  cluster. `tests/csi/snapshot-crds.sh` installs both.

## 1. Build the image and the chart

One image carries the driver (`constellation-csi --controller` / `--node`)
and the `constellation` engine binary the engine pods run:

```bash
make csi-image-dist CSI_IMAGE=registry.example.com/constellation-csi:0.1.0
make csi-chart
```

`csi-image` builds the static musl image; `csi-image-dist` also saves it to
`target/dist/csi/<image>.tar.gz` (`docker load -i` it on an air-gapped
host). `csi-chart` lints and checks the chart (`tests/csi/chart-check.sh`)
and packages it to `target/dist/csi/constellation-csi-<version>.tgz`.
Neither pushes anything. Publishing is in
[Releasing](development/RELEASING.md#kubernetes-csi-image-and-chart).

## 2. Install

The node plugin is privileged, so its namespace must admit privileged pods
(PodSecurity is per namespace):

```bash
kubectl create namespace constellation-csi
kubectl label namespace constellation-csi pod-security.kubernetes.io/enforce=privileged
helm upgrade --install constellation-csi target/dist/csi/constellation-csi-0.1.0.tgz \
    -n constellation-csi \
    --set image.repository=registry.example.com/constellation-csi --set image.tag=0.1.0 \
    -f my-values.yaml
kubectl get csidriver csi.constellation.dev
```

`values.yaml` documents every setting: sidecar versions, the engine pods'
resources and idle TTL, the handover timeouts, the purge worker's budgets
and the classes the chart creates. Numbers may be given with `--set`, in a
values file or through `helm upgrade --reuse-values`; the chart renders
each as an integer, and refuses a fractional one.

The credentials Secret must live in the **release namespace**:
external-provisioner reads it with the controller's ServiceAccount, which
may read Secrets only there.

```bash
kubectl -n constellation-csi create secret generic constellation-s3-creds \
    --from-literal=aws_access_key_id=... --from-literal=aws_secret_access_key=... \
    --from-literal=e2e_passphrase=...      # only for an e2e: "true" class
```

## 3. Choose a layout

Every `StorageClass` is one isolation unit. It is either one **pool** (all
its volumes are subtrees `/volumes/<pv>` of one shared Constellation
filesystem, optionally sharded) or **dedicated** (each volume is a
filesystem of its own).

| | `layout: pool` (default) | `layout: dedicated` |
|---|---|---|
| Snapshots and clones | Metadata-only, between any two volumes of the pool (same shard) | Snapshots of the volume itself. No clone or restore into another volume (that would be a full copy, which the driver refuses) |
| Idle cost per node | One engine pod and one metadata tailer per (pool, node), however many volumes | One engine pod per volume per node |
| Cache, dedup | One cache and one dedup domain per pool | None shared |
| Isolation | One E2E key, one commit chain, one GC domain for the whole pool; the engine pod can reach every volume of the pool | Its own filesystem, key and failure domain |
| Deleting a volume | `DeleteVolume` moves it to `/.trash/`; the purge worker empties it (`purge.*` values) | Refused (`FAILED_PRECONDITION`): the driver never drops a whole filesystem. Use `reclaimPolicy: Retain` and remove the prefix by hand |

**The `StorageClass` is the trust boundary.** Subtree confinement keeps a
volume's mount inside its directory, but it is not a security boundary
between tenants: a pool's engine pod holds the whole pool. Give tenants that
must not share a failure or trust domain their own class, with their own
bucket prefix and credentials (or `layout: dedicated`). Separate PVCs alone
are not enough.

## 4. StorageClass examples

These are plan 37 §6's examples, adjusted to what the driver checks: Secrets
in the release namespace, and `Retain` for dedicated volumes. Unknown
parameters are refused (`INVALID_ARGUMENT` naming the key). The driver
ignores `mountOptions`.

```yaml
# Example 1: a pool StorageClass (the common case), unsharded.
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: constellation-rwx
provisioner: csi.constellation.dev
parameters:
  bucket: "constellation-csi-pool"          # required
  # Default "constellation-csi" for every class: two classes in one bucket
  # must set distinct prefixes, or they share one pool.
  prefix: "constellation-csi/constellation-rwx"
  # endpoint: "https://s3.us-west-2.amazonaws.com"
  # region: "us-west-2"
  layout: "pool"
  shards: "1"
  # Filesystem-creation defaults, applied when the pool is created.
  chunkSize: "4MiB"
  e2e: "true"
  writeMode: "strict"
  engineProfile: "server"                   # the only supported value
  csi.storage.k8s.io/provisioner-secret-name: "constellation-s3-creds"
  csi.storage.k8s.io/provisioner-secret-namespace: "constellation-csi"
  csi.storage.k8s.io/node-stage-secret-name: "constellation-s3-creds"
  csi.storage.k8s.io/node-stage-secret-namespace: "constellation-csi"
  # The same Secret again: a plugin restart followed by an engine crash
  # then heals at the next publish (chart README, "Credentials").
  csi.storage.k8s.io/node-publish-secret-name: "constellation-s3-creds"
  csi.storage.k8s.io/node-publish-secret-namespace: "constellation-csi"
  csi.storage.k8s.io/controller-expand-secret-name: "constellation-s3-creds"
  csi.storage.k8s.io/controller-expand-secret-namespace: "constellation-csi"
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
---
# Example 2: a sharded pool. Many small, short-lived PVs (a CI system's
# scratch volumes) spread over 4 pool filesystems for metadata throughput.
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
  csi.storage.k8s.io/provisioner-secret-namespace: "constellation-csi"
  csi.storage.k8s.io/node-stage-secret-name: "constellation-s3-creds"
  csi.storage.k8s.io/node-stage-secret-namespace: "constellation-csi"
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
---
# Example 3: dedicated layout. One filesystem per PV at <prefix>/<pv-name>,
# its own E2E key; a tenant's own bucket and credentials.
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
  csi.storage.k8s.io/provisioner-secret-namespace: "constellation-csi"
  csi.storage.k8s.io/node-stage-secret-name: "tenant-a-s3-creds"
  csi.storage.k8s.io/node-stage-secret-namespace: "constellation-csi"
reclaimPolicy: Retain          # DeleteVolume of a dedicated volume is refused
allowVolumeExpansion: true
volumeBindingMode: Immediate
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: constellation-snapshots
driver: csi.constellation.dev
deletionPolicy: Delete
parameters:
  csi.storage.k8s.io/snapshotter-secret-name: "constellation-s3-creds"
  csi.storage.k8s.io/snapshotter-secret-namespace: "constellation-csi"
```

The chart can create classes for you (`storageClasses`,
`volumeSnapshotClasses` in `values.yaml`, same fields). Access modes: RWO,
RWOP, ROX and **RWX** across nodes. RWX gives close-to-open consistency:
a file closed on one node is seen whole by an `open` on another. It is not
NFS-style cache coherence for a file open on two nodes at once.

## 5. How many shards

Sharding raises **metadata throughput**. It does not raise a volume count
(K0 Track B, plan 37 "K0 results"):

- One pool filesystem sustained **about 1.2–1.4k volume-creation sequences
  per second** (1377/s with 64 `CreateVolume`s in flight, no errors).
- No PV-count limit was found: a pool grown one volume at a time was clean
  through **10,000 volumes**.

So start every class at `shards: "1"`. Use `shards: "N"` (about N times
the rate, at most 64) only for a class whose volume churn (creates,
deletes, clones per second, across the cluster) approaches that rate.
Every shard costs an engine pod and a metadata tailer on each node that
mounts one of its volumes. Decide when you create the class: a volume's
shard is fixed by a hash of its name, `StorageClass` parameters cannot
change, and clones and restores stay in their source's shard (a class with
another shard count is refused for them).

## 6. Check the install

```bash
helm upgrade constellation-csi <chart> -n constellation-csi --reuse-values \
    --set tests.storageClassName=constellation-rwx
helm test constellation-csi -n constellation-csi
```

The test binds a 64 MiB PVC of the class. A pod writes 4 MiB, a checksum
and a directory, and reads them back. A second pod then mounts the volume
afresh, verifies it and empties it. Helm deletes all three once the test
passes; the volume goes to the pool's trash. Without a class
(`tests.storageClassName`, or the first of `storageClasses`) the test
fails and says what to set.

## 7. Snapshots and clones

A `VolumeSnapshot` of a pool volume is a Constellation snapshot of its
subtree, held by `csi:<VolumeSnapshotContent uid>`: retention policies and
pruning never touch it, and `constellation snapshot ls` shows it held by
CSI. Restoring (`dataSource: VolumeSnapshot`) and cloning
(`dataSource: PersistentVolumeClaim`) are metadata-only copies inside the
source's filesystem. They take seconds whatever the size. The new PVC
must use a class of the **same pool and shard count**. Across pools,
shards or into a dedicated class the request fails with
`ProvisioningFailed … InvalidArgument` naming both filesystems and stays
`Pending`: the driver never makes a silent full copy.

## 8. Static provisioning (existing data)

Any existing path inside a pool can be a `PersistentVolume`, typically a
dataset someone copied in by hand (step 9). The `volumeHandle` is
`<pool fs-uuid>/<path>`. The pool's filesystem UUID is in every
dynamically provisioned PV's handle of that class:
`v1/pool/<shard>/<fs-uuid>/volumes/<pv>`. Alternatively, `constellation
status --s3 s3://<bucket>/<prefix>` prints it. `volumeAttributes` locate
the pool, with the class's parameters:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: imagenet-dataset
spec:
  capacity: {storage: 500Gi}
  accessModes: [ReadOnlyMany]
  persistentVolumeReclaimPolicy: Retain      # the driver never deletes it
  storageClassName: ""
  csi:
    driver: csi.constellation.dev
    volumeHandle: "4f9c1e2a-…/datasets/imagenet"
    readOnly: true
    volumeAttributes:
      bucket: "constellation-csi-pool"
      prefix: "constellation-csi/constellation-rwx"
      layout: "pool"
    nodeStageSecretRef: {name: constellation-s3-creds, namespace: constellation-csi}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: imagenet, namespace: ml}
spec:
  storageClassName: ""
  volumeName: imagenet-dataset
  accessModes: [ReadOnlyMany]
  resources: {requests: {storage: 500Gi}}
```

No `CreateVolume` happens; a write through a `ReadOnlyMany` mount fails
`EROFS`. A sharded pool's shard `k` is the filesystem at
`<prefix>/shard-<k>`.

## 9. Mounting a pool from outside Kubernetes

A pool is an ordinary Constellation filesystem. People may mount it with
the CLI to inspect a volume, seed a dataset, or copy a volume's contents
out:

```bash
export AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=…   # AWS_ENDPOINT=… for a non-AWS S3
constellation mount pool:/volumes/pvc-1a2b… /mnt/pv \
    --s3 s3://constellation-csi-pool/constellation-csi/constellation-rwx
constellation mount pool:/ /mnt/pool   # the whole pool: /volumes, /datasets, /.trash
```

`pool` is the local name the mount registers. A sharded pool's shard `k`
is `--s3 s3://<bucket>/<prefix>/shard-<k>`. The rules (plan 37 settled
decision 18):

- **The volume's quota applies to you.** Writes under `/volumes/<pv>/`
  count against the same quota the pod sees (`ENOSPC` past it), and the
  pod's `df` shows your bytes.
- **Your writes and the pod's meet at close-to-open.** Each side sees the
  other's files once they are closed, as two nodes do.
- **Do not delete a volume's contents by hand.** Constellation cannot tell
  your `rm -rf /volumes/<pv>` from the application's own deletes, so
  nothing stops it. The node plugin does notice when the volume's
  directory or its record is gone, and reports the volume abnormal
  (`VolumeRemoved`, see Troubleshooting) instead of serving an empty mount
  as healthy. Delete volumes through their PVC.
- **Do not create, rename or remove anything directly under `/volumes` or
  `/.trash`.** The driver owns those: a non-empty directory there without
  its record is never adopted (`CreateVolume` answers `ALREADY_EXISTS`),
  and the purge worker deletes trash entries. Seed data elsewhere
  (`/datasets/...`) and bind it statically.
- **Leave when you are done for good.** The mount is a node of the pool's
  cluster. `constellation leave` (or `constellation export pool`) removes
  it from the roster; an unmount alone keeps the node record for the next
  mount.

## 10. Credentials

Credentials reach an engine pod only as a control call (`fs.unlock`) over
its socket: never in a pod spec, an environment variable, an image or a
hostPath file. They live in its memory. The class parameter
`credentialSource` picks the source:

- `static-ephemeral` (default): the `csi.storage.k8s.io/*-secret-*` Secret,
  resolved per request.
- `refreshing`: the same Secret, also watched (list it under
  `credentials.watchedSecrets`). Every change reaches every running engine
  pod at once, without a remount.
- `aws-default-chain`: no Secret at all. Give the engine pods'
  ServiceAccount `constellation-csi-engine` an IRSA role annotation
  (`engineServiceAccount.annotations`) or an EKS Pod Identity association.
  This is the preferred production setup.

To rotate a key pair, create the new pair, update the Secret, wait until
every engine pod uses it, then revoke the old pair. The chart README's
"Rotating a key pair" has the exact steps and the fields to check.

## 11. Upgrades

- **Engine pods** (a new image, or a changed `engineProfile` value): each
  node plugin replaces its engine pods one at a time by **handing their
  FUSE sessions over** to the replacement (`node.handoff`, plan 37 §8).
  Writers see a pause, never `ENOTCONN`. K5's gate measured 20 runs under
  a writing, creating and reading load with zero errors. The
  `engineProfile.handoff.*` values bound each step. A pod whose handover
  fails `maxAttempts` times keeps serving (event and annotation
  `constellation.dev/handoff-fallback`), and its volumes move at the next
  republish.
- **Plugin pods** (controller, node DaemonSet): mounts survive a node
  plugin restart. Engine pods hold the FUSE connections, not the plugin.
  `node.maxUnavailable` paces the DaemonSet.
- `helm upgrade --reuse-values` is safe for numeric values.

## 12. SELinux-enforcing nodes

The `CSIDriver` declares `seLinuxMount: false`, and that is the
recommendation for this release:

- One staging mount per (volume, node) is shared by every pod that
  publishes the volume there. With `seLinuxMount: true` kubelet asks for
  each volume to be mounted with the pod's `context=` label, so a second
  pod with a different label cannot use the volume on that node while the
  first runs (unless it opts out with `seLinuxChangePolicy: Recursive`).
  For RWX volumes shared across namespaces, whose pods get different MCS
  labels by default, that is a regression.
- The driver does not apply mount options yet, so declaring `true` would
  make kubelet skip relabeling a mount that carries no context.

On SELinux-enforcing nodes, FUSE mounts are labeled `fusefs_t`, and
`container_t` processes need the `container-selinux` boolean to use them:

```bash
setsebool -P virt_sandbox_use_fusefs on   # on every node
```

That admits every FUSE filesystem on the node to containers, not one
volume. Clusters that need per-volume labels wait for the follow-up
(PROGRESS, plan 37 close-out, F7): honour a class-wide `context=` mount
option at `NodeStageVolume`, verify it on an enforcing node, then declare
`seLinuxMount: true`.

## Troubleshooting

**Find the pods.** Engine pods are `constellation-engine-<unit>-<node>`
(node-owned, serving mounts) and `constellation-engine-<unit>-controller`
(the controller's, for provisioning and purge), labelled with the pool's
`fs-uuid`:

```bash
kubectl -n constellation-csi get pods -o wide
kubectl -n constellation-csi logs <engine-pod>
kubectl -n constellation-csi logs deploy/constellation-csi-controller -c constellation-csi
kubectl -n constellation-csi logs <node-plugin-pod> -c constellation-csi
```

**An engine's live status** (`node.status`: every mounted volume, the
journal backlog, the cache, the lease, P2P, the version):

```bash
kubectl -n constellation-csi exec <engine-pod> -c engine -- \
    constellation status pool --state-dir /var/lib/constellation/state
```

Standard output is the `node.status` JSON (pipe it to `jq`); a human
summary of the snapshot scheduler and accounting goes to standard error.
The command finds the engine's control socket through the state
directory's `control.path`.

`spool.journal_backlog` that keeps growing means the engine cannot ship to
S3 (check `spool.last_ship_error`). `spool.conflicts` above 0 means two
writers raced without a lease and is worth reporting.

**Who did what** (the engine's control audit log, one JSON line per call,
naming the caller and the PV it was about):

```bash
kubectl -n constellation-csi exec <engine-pod> -c engine -- \
    tail -n 20 /var/lib/constellation/state/control-audit.jsonl
```

The node plugin appears as `"kind":"service","label":"csi-node-plugin"`,
the controller as `csi-controller`, and `"on_behalf_of":"pvc-…"` names the
volume. A refused credential rotation is there too, with the parameters'
digest withheld.

**A volume's health** (what kubelet would get from `NodeGetVolumeHealth`),
asked of the node plugin on the node that stages it:

```bash
kubectl -n constellation-csi exec <node-plugin-pod> -c constellation-csi -- \
    constellation-csi --endpoint unix:///csi/csi.sock --volume-health <volume-handle>
```

`DATA_LOSS` / `VolumeRemoved` means the volume's directory or record is
gone (step 9).

**Common failures:**

| Symptom | Cause |
|---|---|
| PVC `Pending`, `ProvisioningFailed … forbidden` | The provisioner Secret is not in the release namespace |
| `… InvalidArgument … parameter` | A typo in a class parameter; the message names the key |
| `… InvalidArgument` on a clone or restore | The class names another pool, shard count or layout (step 7) |
| Pod `ContainerCreating`, `MountVolume.MountDevice failed … UNAVAILABLE` | The engine pod is not ready: its image pull, or credentials it has not got. `kubectl describe` the engine pod; with `static-ephemeral`, name the Secret as the node-publish secret too |
| Engine pod waiting, log says the credentials were refused | Wrong key pair or passphrase in the Secret. The pod keeps waiting and never starts on bad credentials |
| `kubectl drain` waits on engine pods | Expected while their volumes are still mounted: the workload pods go first, then the node plugin removes the idle engine pods (`node.drain.*`) |
| Deleted volumes' space comes back late | The purge worker empties `/.trash` every `purge.interval` after `purge.grace`, within `purge.opsPerSecond` / `bytesPerSecond`, then bucket GC reclaims the chunks |
