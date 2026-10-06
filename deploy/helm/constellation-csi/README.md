# constellation-csi

The Kubernetes CSI driver for Constellation (plan 37,
`docs/plans/v1/done/37-kubernetes-csi.md`): a controller `Deployment`, a node
plugin `DaemonSet`, and the engine pods both start — one per pool filesystem
for the controller, one per (pool filesystem, node) for each node plugin.
Everything is in the release namespace.

```bash
helm upgrade --install constellation-csi deploy/helm/constellation-csi \
    -n constellation-csi --create-namespace
kubectl label namespace constellation-csi pod-security.kubernetes.io/enforce=privileged
```

The how-to guide, `docs/how-to-guides/kubernetes-csi.md`, walks through a
deployment: the classes (pool, sharded pool, dedicated), static
provisioning, snapshots and clones, upgrades and troubleshooting.
`values.yaml` documents every value. Numbers may come from `--set`, a
values file or `helm upgrade --reuse-values`: each renders as an integer,
and a fractional one is refused (`tests/csi/chart-check.sh`).

## Building, checking, publishing

```bash
make csi-image CSI_IMAGE=constellation-csi:dev        # the image, in the local docker
make csi-image-dist CSI_IMAGE=constellation-csi:dev   # and target/dist/csi/constellation-csi_dev.tar.gz
make csi-chart                                        # lint + render checks, then target/dist/csi/constellation-csi-<version>.tgz
```

Nothing pushes. The publish commands (`docker push`, `helm push
oci://…`) are in `docs/how-to-guides/development/RELEASING.md`.

## Checking an install: `helm test`

```bash
helm test constellation-csi -n constellation-csi
```

`templates/tests/volume-roundtrip.yaml` binds a PVC of
`tests.storageClassName` (default: the first of `storageClasses`). A pod
writes 4 MiB, a checksum and a directory, and reads them back. A second pod
then mounts the volume afresh, verifies it and empties it. Helm deletes all
three when the test passes; the volume goes to the pool's trash. A failed
test leaves them for `kubectl logs`. Without a class the test fails and
says what to set. `tests/csi/e2e.sh` runs it on every nightly.

## Pools: the trust model and sharding

A `layout: pool` class is one Constellation filesystem shared by all its
volumes (subtrees `/volumes/<pv>`): one E2E key, one metadata commit
chain, one GC domain, one engine pod per node for all of them. A subtree
view confines each volume's mount, but the engine pod can reach the whole
pool, so **the `StorageClass` is the isolation boundary**. Tenants that
must not share a failure or trust domain need their own class (their own
prefix and credentials, or `layout: dedicated`), not just their own PVCs.

`shards: N` spreads a class's volumes over N pool filesystems by a hash of
the volume name, for **metadata throughput**. One pool filesystem
sustained about 1.2–1.4k volume-creation sequences per second (plan 37
K0 Track B: 1377/s, 64 in flight, no errors), and no PV-count limit was
found up to 10,000 volumes. Start at 1, and shard a class whose volume
churn approaches that rate. Choose N when creating the class: clones and
restores stay in their source's shard.

## Mounting a pool outside Kubernetes

People may mount a pool with the ordinary CLI to inspect a volume, seed a
dataset for static provisioning or copy a volume out:

```bash
constellation mount pool:/volumes/pvc-1a2b… /mnt/pv --s3 s3://<bucket>/<prefix>[/shard-<k>]
```

The rules (plan 37 settled decision 18):

- The volume's quota governs the person's writes exactly as the pod's
  (`ENOSPC` past it), and both see each other's closed files
  (close-to-open).
- Deleting a volume's contents by hand is not prevented (Constellation
  cannot tell an admin's `rm -rf` from the application's deletes), but it
  is detected. The node plugin reports the volume abnormal (`DATA_LOSS`,
  `VolumeRemoved` from `NodeGetVolumeHealth`) instead of serving an empty
  mount as healthy. Delete volumes through their PVC.
- Leave `/volumes` and `/.trash` themselves to the driver. Seed datasets
  elsewhere (`/datasets/…`) and bind them with a static PV.
- `constellation leave` once done: the mount was a node of the pool's
  cluster.

## SELinux

The `CSIDriver` declares `seLinuxMount: false`. Every pod that publishes a
volume on a node shares its one staging mount, and the driver does not
apply mount options. On SELinux-enforcing nodes let containers use FUSE
mounts with `setsebool -P virt_sandbox_use_fusefs on` (all FUSE mounts on
the node, not per volume). The reasoning, and the follow-up that would
allow `true`, are in the how-to guide.

## Who is privileged (plan 37 §9)

| Pod | Privileged | PodSecurity level it meets | Why |
|---|---|---|---|
| node plugin (`constellation-csi-node-*`) | **yes**: `privileged: true`, uid 0, `Bidirectional` mount propagation | `privileged` only | It alone calls `mount(2)` (the FUSE staging mount, `fuse_mount_fd`) and the publish bind mounts. Nothing weaker works (plan 37 "K0 results", question 5). |
| controller (`constellation-csi-controller-*`) | no | `restricted` | Non-root (65532), every capability dropped, no privilege escalation, read-only root, `RuntimeDefault` seccomp, only an `emptyDir`. |
| controller-owned engine pod (`constellation-engine-<unit>-controller`) | no | `restricted` | As the controller; its state and control socket are `emptyDir`s (it never serves a view, and the controller reaches it through an exec relay), so no hostPath and no init container. |
| node-owned engine pod (`constellation-engine-<unit>-<node>`) | no | `restricted`'s container rules; `privileged` for its volumes | Same container security context as above, but its meta store, control socket and allowlist are the node's `<hostRoot>/{node-identity,sockets,policy}/<unit>` hostPaths, which no PodSecurity level below `privileged` admits. The node plugin makes those directories owned by uid 65532 before it creates the pod (`type: Directory`), so the pod needs no root init container to `chown` them. |

PodSecurity admission is per namespace, so the release namespace has to be
`privileged` for the node plugin (and the node-owned engine pods' hostPaths).
The other pods do not rely on that: each declares the `restricted`
security context itself, and the chart's ValidatingAdmissionPolicies
(`templates/exec-policy.yaml`, `controller.podAccessPolicy` and
`node.podAccessPolicy`, Kubernetes ≥ 1.30) refuse any engine pod the plugins'
ServiceAccounts create that is not exactly that shape: no init container,
no added capability, no privilege escalation, a read-only root,
`RuntimeDefault` seccomp, no host namespace, no environment from a Secret,
only the unit's own volumes. The harness checks the split the way admission
sees it: `harness k8s-scenario csi-pod-security` re-creates every driver pod
by server dry run in a namespace that enforces `restricted` — the
controller and the controller-owned engine pod are admitted, a node-owned
engine pod is refused for its hostPath volumes and nothing else, the node
plugin is refused as privileged.

Moving the engine pods to a namespace of their own would not make the
node-owned ones `restricted` (their hostPaths stay), so the chart keeps one
namespace and documents the trade-off instead (plan 37 §9).

## Credentials

An engine pod's S3 credentials and E2E passphrase are **never** in its pod
spec, its environment, its image, a log line or a hostPath file. The pod
runs `constellation serve --await-unlock`: until its plugin sends `fs.unlock`
over the control socket, the socket answers only `node.ping` (the readiness
probe) and `fs.unlock`; the credentials then live in the engine's in-memory
`EphemeralSecretStore`, and every later `fs.unlock` rotates them in place —
the S3 clients sign their next request with the new keys, no remount. Every
`fs.unlock` is in the engine's audit log (`control-audit.jsonl` in its state
dir) with the parameters' digest withheld; a rotation pushed from a watched
Secret carries `on_behalf_of: "secret:<namespace>/<name>"`.

Before a running engine swaps in a rotated key pair it reads its
`meta.json` with the new pair. If S3 refuses it (a typo in the rotated
Secret, a revoked key) the `fs.unlock` is refused (`Denied`, recorded so in
the audit line) and the engine keeps the pair it has; if S3 could not be
asked, the plugin retries with backoff. An engine pod waiting for its first
`fs.unlock` checks the credentials the same way (and an E2E passphrase
against the keyring) and keeps waiting on a wrong one instead of starting.
No S3 error body ever reaches a log line, a control error, a gRPC status or
a kubelet event: S3's authentication errors echo the request's key id and
string to sign, so every S3 client keeps only the error's status and code
(`403 … SignatureDoesNotMatch: S3 refused the credentials`). The engine and
both plugins run with core dumps off (`RLIMIT_CORE` 0, not dumpable): the
credentials they hold in memory never land in a core file.

The StorageClass parameter `credentialSource` picks where they come from:

| `credentialSource` | Credentials | Rotation |
|---|---|---|
| `static-ephemeral` (default) | The Secret named by `csi.storage.k8s.io/{provisioner,node-stage,node-publish,controller-expand}-secret-{name,namespace}` (and a VolumeSnapshotClass's `csi.storage.k8s.io/snapshotter-secret-*`), resolved per request by the sidecars and kubelet. | A changed Secret reaches running engine pods with the next request that carries it (a `CreateVolume`, a `NodeStageVolume`). |
| `refreshing` | The same, plus `credentialSecretName`/`credentialSecretNamespace` naming the Secret to watch. List it under `credentials.watchedSecrets`, which grants both plugins `get`/`list`/`watch` on exactly that Secret (by name). | The plugins watch the Secret and push every change to every running engine pod at once (`harness k8s-scenario csi-secret-rotation`). |
| `aws-default-chain` | None through the driver: the engine pods' own ServiceAccount, `constellation-csi-engine` — give it IRSA's `eks.amazonaws.com/role-arn` annotation (`engineServiceAccount.annotations`) or an EKS Pod Identity association; the pod-access policies admit the projected token the EKS webhook injects. An E2E class still needs a secret carrying `e2e_passphrase`. | The AWS SDK renews its own credentials. |

`aws-default-chain` is the preferred production path: no secret ever transits
the driver.

The plugins remember the last credentials they sent each engine pod, in
memory only, to unlock a replacement after an engine crash (a restage from a
`NodePublishVolume`), and forget them once no volume on that node uses the
pod. A node plugin that restarted since then has none for a
`static-ephemeral` class, and a publish carries a secret only when the class
names one in `csi.storage.k8s.io/node-publish-secret-name` (and
`-namespace`): **name the same Secret there** as in `node-stage-secret-*`,
and a plugin restart (routine during upgrades) followed by an engine crash
heals at the next publish. Without it the restage fails `UNAVAILABLE`
(saying why) until kubelet stages the volume again with its secret —
recreate the pod that uses it. A `refreshing` class re-reads its Secret
instead.

### Rotating a key pair

In-flight S3 requests, and the retries of a request already signed, keep
the old signature for a while after a rotation, so revoke the old key only
once nothing uses it:

1. Create the new key pair in S3 (IAM, the S3 service's user admin); keep
   the old one.
2. Update the Secret (`kubectl apply`, or the external-secrets operator).
   A `refreshing` class pushes it to every running engine pod at once; each
   checks it against the bucket before using it. A `static-ephemeral` class
   gets it with the next request that carries it.
3. Wait until every engine pod uses it: the control method `fs.list` on
   each pod's socket reports `credentials_generation` (what the engine holds) and
   `credentials_in_use` (what its S3 clients last signed with); they are
   equal once the new pair is in use. An engine that refused the pair keeps
   the old generation and logs why.
4. Revoke the old key pair.

`harness run csi-credential-revocation` runs this sequence against
versitygw, which checks every signature: the old pair revoked, I/O carries
on, and a rotation to the revoked pair or a wrong secret is refused while
the pair in use stays.

RBAC: the controller may `get` Secrets in the release namespace only (what
external-provisioner and external-resizer read with its ServiceAccount, and
a lost engine pod's rebuild); the node plugin has no Secret permission;
neither may create or change one. `credentials.watchedSecrets` adds exactly
the named Secrets, for both.

## Control-socket identity

Each node-owned engine pod reads a one-row allowlist the node plugin writes
into the root-owned `<hostRoot>/policy/<unit>/control-allow.toml` (mounted
read-only): a `kind = "service"` grant for uid 0 on that pod's own socket,
role `admin` (`view.mount`, `view.unmount` and `fs.unlock` are admin-only
methods), label `csi-node-plugin`. The controller reaches its engine pods
through `kubectl exec` of `constellation control-relay`, which runs as the
engine's own uid (65532); a controller-owned engine pod reads the image's
`/etc/constellation-csi/controller-engine/control-allow.toml`
(`CONSTELLATION_CONTROL_POLICY`), one `kind = "service"` grant for uid
65532 on its socket, role `admin`, label `csi-controller`, so the
controller's calls are attributed to that service rather than to the bare
owner uid. One call needs that grant, not just the role: a serving engine
hands its `fs.unlock` credentials to a replacement's handoff (the
`node.handoff` `Credentials` step) only to the caller that matched the
`csi-node-plugin` grant, and only while a replacement waits — never to the
owner uid, which is what `kubectl exec` into an engine pod runs as. Every call the plugins make carries
the PersistentVolume it is about (`on_behalf_of`), so an engine's audit
line reads, for instance:

```json
{"principal":{"kind":"service","uid":0,"socket":"/run/constellation-csi/control.sock","label":"csi-node-plugin"},
 "role":"admin","method":"view.mount","outcome":"ok","on_behalf_of":"pvc-1a2b…", …}
```

A note for lock fencing (b458669): under the node plugin, FUSE requests from
application pods reach the engine with **pid 0** — the kernel cannot name
the caller in the engine pod's pid namespace. The owner fence's process
match skips pid 0 (`ClusterLocks::owner_fenced` keeps only owners with a pid
above 1), so a lapsed cluster-lock owner in an application pod is fenced by
what does not depend on the pid — its `fcntl` lock-owner token on direct
I/O and the inode's discard of unpublished data — never by process, and no
other pod's process is ever fenced by mistake. The engine never acts on a
process it cannot see: an engine pod cannot signal or inspect an
application's processes.
