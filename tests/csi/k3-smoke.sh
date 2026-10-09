#!/usr/bin/env bash
# Plan 37 K3 gate on kind: a pod mounts a PV and reads and writes through it.
#
#   1. kind cluster (tests/csi/kind-up.sh) + the chart; a private floci S3 on
#      the `kind` network; a pool StorageClass with provisioner and
#      node-stage secrets;
#   2. PVC a (RWX) → Bound; pod a1 on worker 1 writes a file through it and
#      reads it back;
#   3. pod a2 on worker 2 reads that file (RWX across nodes, close-to-open)
#      and appends to it; pod a3 on worker 1 sees the append;
#   4. PVC b in the same pool, mounted by pod b1 on worker 1 next to a3:
#      still exactly one node-owned engine pod per (pool, node) — two
#      pods, one per worker, not one per PV — pinned, unprivileged, owned
#      by the node DaemonSet, its view count annotated;
#   5. settled decision 12: the worker-1 engine pod is killed; kubelet's
#      republish (requiresRepublish) restages PVC a on a new incarnation,
#      and a new pod on worker 1 reads the file again;
#   5a. plan 37 §9 negatives: the node ServiceAccount cannot read Secrets;
#      with a real token of the node plugin on worker 2 it cannot delete or
#      annotate worker 1's engine pod, exec into one, or create an
#      engine-shaped pod running `sh`, on worker 1, or mounting another host
#      directory (each against a positive control);
#   5b. the 37-k3a symlink exploit: the engine plants links where its grant
#      used to be written and cannot touch where it is now; a forced
#      rewrite of the grant leaves the victim file untouched;
#   6. every pod deleted: the volumes unstage — no FUSE mount left on the
#      nodes, the engine pods annotated idle (GC is 37-k6b's).
#
# Usage: tests/csi/k3-smoke.sh
# Knobs:
#   KIND_BIN, KIND_CLUSTER (default k3-smoke), KIND_CONFIG_DRAFT,
#   KIND_NODE_IMAGE      as for kind-up.sh
#   CSI_IMAGE            image to load (default constellation-csi:dev; built
#                        with `make csi-image` unless CSI_SKIP_BUILD=1)
#   CSI_KEEP=1           leave the cluster and the S3 container running
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/csi/lib.sh"
kind="${KIND_BIN:-kind}"
export KIND_CLUSTER="${KIND_CLUSTER:-k3-smoke}"
export CSI_IMAGE="${CSI_IMAGE:-constellation-csi:dev}"
export CSI_NAMESPACE="${CSI_NAMESPACE:-constellation-csi}"
ctx="kind-$KIND_CLUSTER"
ns="$CSI_NAMESPACE"
s3c="$KIND_CLUSTER-floci"
bucket=k3-smoke
w1="$KIND_CLUSTER-worker"
w2="$KIND_CLUSTER-worker2"
k() { kubectl --context "$ctx" "$@"; }

cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        echo "== FAILED; diagnostics"
        k get pods -o wide 2>/dev/null || true
        k -n "$ns" get pods -o wide 2>/dev/null || true
        k get pvc,pv 2>/dev/null || true
        k -n "$ns" logs -l app.kubernetes.io/component=node -c constellation-csi --tail=60 \
            --prefix 2>/dev/null || true
        k -n "$ns" logs -l app.kubernetes.io/component=engine -c engine --tail=30 \
            --prefix 2>/dev/null || true
    fi
    if [ "${CSI_KEEP:-0}" != 1 ]; then
        "$kind" delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
        docker rm -f "$s3c" >/dev/null 2>&1 || true
    fi
    exit "$status"
}
trap cleanup EXIT

build_image "$CSI_IMAGE"
echo "== kind cluster $KIND_CLUSTER + chart"
"$root/tests/csi/kind-up.sh" --install

echo "== S3 (floci) on the kind network"
endpoint=$(floci_up "$s3c" "$bucket")
echo "   endpoint $endpoint, bucket $bucket"

echo "== pool StorageClass, credentials"
k -n "$ns" create secret generic constellation-s3-creds \
    --from-literal=aws_access_key_id=test --from-literal=aws_secret_access_key=test \
    --dry-run=client -o yaml | k apply -f - >/dev/null
k apply -f - >/dev/null <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: k3-pool
provisioner: constellation.csi.replicix.com
parameters:
  bucket: "$bucket"
  prefix: "constellation-csi/k3-pool"
  endpoint: "$endpoint"
  region: "us-east-1"
  layout: "pool"
  chunkSize: "1MiB"
  csi.storage.k8s.io/provisioner-secret-name: constellation-s3-creds
  csi.storage.k8s.io/provisioner-secret-namespace: "$ns"
  csi.storage.k8s.io/node-stage-secret-name: constellation-s3-creds
  csi.storage.k8s.io/node-stage-secret-namespace: "$ns"
reclaimPolicy: Delete
volumeBindingMode: Immediate
EOF

pvc() {
    k apply -f - >/dev/null <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: $1, namespace: default}
spec:
  accessModes: [ReadWriteMany]
  storageClassName: k3-pool
  resources: {requests: {storage: 1Gi}}
EOF
    k wait --for=jsonpath='{.status.phase}'=Bound "pvc/$1" --timeout=300s >/dev/null
    echo "   pvc/$1 Bound ($(k get pvc "$1" -o jsonpath='{.spec.volumeName}'))"
}

# pod NAME NODE SCRIPT PVC...: a pod pinned to NODE, every PVC at /data/<pvc>.
pod() {
    local name="$1" node="$2" script="$3"
    shift 3
    local mounts="" vols=""
    for c in "$@"; do
        mounts+="        - {name: $c, mountPath: /data/$c}"$'\n'
        vols+="    - {name: $c, persistentVolumeClaim: {claimName: $c}}"$'\n'
    done
    k apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata: {name: $name, namespace: default}
spec:
  nodeName: $node
  restartPolicy: Never
  terminationGracePeriodSeconds: 1
  # Not root: the volume becomes writable through fsGroupPolicy File —
  # kubelet's recursive chgrp/chmod after NodePublishVolume, through FUSE.
  securityContext: {runAsUser: 1000, runAsGroup: 1000, fsGroup: 1000}
  containers:
    - name: c
      image: $CSI_IMAGE
      imagePullPolicy: IfNotPresent
      command: [sh, -c, '$script; sleep 3600']
      volumeMounts:
$mounts  volumes:
$vols
EOF
    k wait --for=condition=Ready "pod/$name" --timeout=240s >/dev/null
}
in_pod() { k exec "$1" -- sh -c "$2"; }

echo "== pvc a; pod a1 on $w1 writes and reads back"
pvc k3-a
pod a1 "$w1" 'true' k3-a
in_pod a1 'echo hello-from-a1 > /data/k3-a/greeting && cat /data/k3-a/greeting' | grep -qx hello-from-a1 \
    || { echo "a1 could not write and read back through the PV"; exit 1; }
in_pod a1 'dd if=/dev/urandom of=/data/k3-a/blob bs=1M count=8 2>/dev/null && sha256sum /data/k3-a/blob' >/tmp/k3-blob.sha
echo "   a1 wrote greeting + 8 MiB blob ($(cut -c1-16 /tmp/k3-blob.sha)…)"

echo "== pod a2 on $w2 reads it (RWX across nodes) and appends"
pod a2 "$w2" 'true' k3-a
in_pod a2 'cat /data/k3-a/greeting' | grep -qx hello-from-a1 || { echo "a2 does not see a1's write"; exit 1; }
in_pod a2 'sha256sum /data/k3-a/blob' | diff -q - /tmp/k3-blob.sha >/dev/null \
    || { echo "a2 reads a different blob"; exit 1; }
in_pod a2 'echo appended-by-a2 >> /data/k3-a/greeting'
pod a3 "$w1" 'true' k3-a
for _ in $(seq 30); do
    in_pod a3 'cat /data/k3-a/greeting' | grep -qx appended-by-a2 && break
    sleep 2
done
in_pod a3 'cat /data/k3-a/greeting' | grep -qx appended-by-a2 || { echo "a3 never saw a2's append"; exit 1; }
echo "   a2 on $w2 read a1's data and appended; a3 on $w1 sees the append"

echo "== pvc b in the same pool next to a3: one engine pod per (pool, node)"
pvc k3-b
pod b1 "$w1" 'true' k3-b
in_pod b1 'echo b > /data/k3-b/own && cat /data/k3-b/own' | grep -qx b \
    || { echo "b1 cannot use pvc b"; exit 1; }
in_pod b1 'test ! -e /data/k3-b/greeting' || { echo "pvc b sees pvc a's files"; exit 1; }
sel=app.kubernetes.io/component=engine,constellation.replicix.com/owner=node
k -n "$ns" get pods -l "$sel" -o wide
engines=$(k -n "$ns" get pods -l "$sel" -o json)
jq -e --arg w1 "$w1" --arg w2 "$w2" '
    (.items | length) == 2
    and ([.items[].spec.nodeName] | sort) == ([$w1, $w2] | sort)
    and all(.items[]; .spec.containers[0].securityContext.privileged != true
                     and .spec.securityContext.runAsNonRoot == true
                     and .metadata.ownerReferences[0].kind == "DaemonSet")' \
    <<<"$engines" >/dev/null || { echo "not one pinned, unprivileged engine pod per (pool, node)"; exit 1; }
views() {
    k -n "$ns" get pods -l "$sel,constellation.replicix.com/node=$1" \
        -o jsonpath='{.items[0].metadata.annotations.constellation\.dev/last-view-count}'
}
[ "$(views "$w1")" = 2 ] || { echo "the $w1 engine pod serves $(views "$w1") views, not 2"; exit 1; }
[ "$(views "$w2")" = 1 ] || { echo "the $w2 engine pod serves $(views "$w2") views, not 1"; exit 1; }
echo "   2 engine pods for 2 PVs on 2 nodes; $w1 serves 2 views, $w2 serves 1"

echo "== plan 37 §9: what the node plugin's ServiceAccount may not do"
nsa="system:serviceaccount:$ns:constellation-csi-node"
for verb in get list create patch; do
    [ "$(k -n "$ns" auth can-i "$verb" secrets --as="$nsa" 2>/dev/null)" = no ] \
        || { echo "the node ServiceAccount may $verb secrets"; exit 1; }
done
for vap in "constellation-csi-node-pods.$ns" "constellation-csi-controller-pods.$ns"; do
    warn=$(k get validatingadmissionpolicy "$vap" -o jsonpath='{.status.typeChecking.expressionWarnings}')
    [ -z "$warn" ] || { echo "$vap does not type-check: $warn"; exit 1; }
done
# A real token of the node plugin on $w2 (bound to its pod, so it carries
# the node name), used from outside: what a thief of that token gets.
np2=$(k -n "$ns" get pods -l app.kubernetes.io/component=node \
    --field-selector "spec.nodeName=$w2" -o jsonpath='{.items[0].metadata.name}')
tok=$(k -n "$ns" create token constellation-csi-node --bound-object-kind Pod \
    --bound-object-name "$np2" --duration 10m)
srv=$(k config view --minify --raw -o jsonpath='{.clusters[0].cluster.server}')
ca=$(mktemp /tmp/k3-ca.XXXXXX)
k config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' \
    | base64 -d >"$ca"
as_w2() { KUBECONFIG=/dev/null kubectl --server="$srv" --certificate-authority="$ca" --token="$tok" "$@"; }
denied() { # denied WHAT CMD...: the call must fail with an admission denial
    local what="$1" out
    shift
    if out=$("$@" 2>&1); then echo "allowed, but must not be: $what"; exit 1; fi
    grep -q 'denied request\|ValidatingAdmissionPolicy' <<<"$out" \
        || { echo "$what failed, but not by the policy: $out"; exit 1; }
    echo "   denied: $what"
}
e1=$(k -n "$ns" get pods -l "$sel,constellation.replicix.com/node=$w1" -o jsonpath='{.items[0].metadata.name}')
e2=$(k -n "$ns" get pods -l "$sel,constellation.replicix.com/node=$w2" -o jsonpath='{.items[0].metadata.name}')
# Positive controls first, so a policy that refuses everything fails here.
as_w2 -n "$ns" delete pod "$e2" --dry-run=server >/dev/null \
    || { echo "the $w2 plugin's token may not delete its own engine pod"; exit 1; }
clone() { # clone JQ: $e2's spec as a new pod, edited by JQ
    k -n "$ns" get pod "$e2" -o json | jq "del(.status, .metadata.uid, .metadata.resourceVersion,
        .metadata.creationTimestamp, .metadata.managedFields, .metadata.ownerReferences)
        | .metadata.name += \"-x\" | $1"
}
clone . | as_w2 -n "$ns" create --dry-run=server -f - >/dev/null \
    || { echo "the $w2 plugin's token may not create its own engine pod's exact shape"; exit 1; }
denied "deleting $w1's engine pod with $w2's token" as_w2 -n "$ns" delete pod "$e1" --dry-run=server
denied "annotating $w1's engine pod with $w2's token" \
    as_w2 -n "$ns" annotate pod "$e1" x=y --dry-run=server
denied "deleting an engine pod with a token naming no node" \
    k -n "$ns" delete pod "$e2" --as="$nsa" --dry-run=server
denied_create() { # denied_create WHAT JQ: $e2's clone, edited by JQ, is refused
    local what="$1" out
    if out=$(clone "$2" | as_w2 -n "$ns" create --dry-run=server -f - 2>&1); then
        echo "allowed, but must not be: $what"
        exit 1
    fi
    grep -q 'denied request\|ValidatingAdmissionPolicy' <<<"$out" \
        || { echo "$what failed, but not by the policy: $out"; exit 1; }
    echo "   denied: $what"
}
denied_create "an engine-shaped pod running sh" '.spec.containers[0].command = ["sh", "-c", "id"]'
denied_create "an engine pod on $w1 from $w2's token" ".spec.nodeName = \"$w1\""
denied_create "an engine pod mounting another host directory" \
    '(.spec.volumes[] | select(.name == "policy") | .hostPath.path) = "/var/lib/kubelet"'
denied_create "an engine pod running as root" '.spec.securityContext.runAsUser = 0'
denied_create "an engine pod with a lifecycle hook" \
    '.spec.containers[0].lifecycle = {postStart: {exec: {command: ["sh", "-c", "id"]}}}'
denied_create "an engine pod with automountServiceAccountToken: true" \
    '.spec.automountServiceAccountToken = true'
denied_create "an engine pod under another ServiceAccount" \
    'del(.spec.serviceAccount) | .spec.serviceAccountName = "constellation-csi-node"'
denied_create "an engine pod with envFrom" \
    '.spec.containers[0].envFrom = [{secretRef: {name: "constellation-s3-creds"}}]'
denied_create "an engine pod reading a Secret into its environment" \
    '.spec.containers[0].env += [{name: "X", valueFrom: {secretKeyRef: {name: "constellation-s3-creds", key: "aws_access_key_id"}}}]'
denied_create "an engine pod with an init container" \
    '.spec.initContainers = [.spec.containers[0] | .name = "init" | del(.startupProbe, .readinessProbe, .livenessProbe)]'
denied_create "an engine pod adding a capability" \
    '.spec.containers[0].securityContext.capabilities.add = ["CHOWN"]'
denied_create "an engine pod whose hostPath kubelet may create" \
    '(.spec.volumes[] | select(.name == "sockets") | .hostPath.type) = "DirectoryOrCreate"'
# exec has no RBAC grant at all, the policy refuses it besides.
if as_w2 -n "$ns" exec "$e2" -c engine -- true >/dev/null 2>&1; then
    echo "allowed, but must not be: exec into an engine pod"
    exit 1
fi
echo "   denied: exec into an engine pod"
rm -f "$ca"

echo "== must-fix 37-k3a: the engine cannot steer the plugin's writes"
unit=$(k -n "$ns" get pod "$e1" -o jsonpath='{.metadata.labels.constellation\.dev/unit}')
hroot=/var/lib/constellation-csi
victim=/var/lib/kubelet/k3a-victim
docker exec "$w1" sh -c "echo precious > $victim && chmod 600 $victim"
# The reviewer's exploit, from the unprivileged engine (uid 65532): links
# where the grant used to be written, and attempts on where it is now.
k -n "$ns" exec "$e1" -c engine -- sh -c "
    ln -sf $victim /run/constellation-csi/.control-allow.toml.tmp
    ln -sf $victim /run/constellation-csi/control-allow.toml
    ! ln -sf $victim /etc/constellation-csi/policy/.control-allow.toml.tmp 2>/dev/null
    ! rm -f /etc/constellation-csi/policy/control-allow.toml 2>/dev/null
    ! { echo x >> /etc/constellation-csi/policy/control-allow.toml; } 2>/dev/null
    test -s /etc/constellation-csi/policy/control-allow.toml" \
    || { echo "the engine could change its policy directory"; exit 1; }
# The plugin writes the grant on every bring-up; make it rewrite (a stale
# grant, as root on the host) and bring up: a new PVC staged on $w1.
docker exec "$w1" sh -c "echo '# stale' > $hroot/policy/$unit/control-allow.toml"
pvc k3-c
pod c1 "$w1" 'true' k3-c
in_pod c1 'echo c > /data/k3-c/own && cat /data/k3-c/own' | grep -qx c \
    || { echo "c1 cannot use pvc c"; exit 1; }
[ "$(docker exec "$w1" cat "$victim")" = precious ] \
    || { echo "the plugin wrote through the engine's symlink into $victim"; exit 1; }
docker exec "$w1" sh -c "test ! -L $hroot/policy/$unit/control-allow.toml \
    && test \"\$(stat -c %u:%a $hroot/policy/$unit $hroot/policy/$unit/control-allow.toml)\" = \
       \"\$(printf '0:755\n0:644')\" \
    && grep -q csi-node-plugin $hroot/policy/$unit/control-allow.toml" \
    || { echo "the grant is not a root-owned regular file in a root-owned directory"; exit 1; }
k delete pod c1 --wait=true >/dev/null
k delete pvc k3-c --wait=true >/dev/null
docker exec "$w1" rm -f "$victim"
echo "   victim untouched; the grant was rewritten in place, root-owned"

echo "== settled decision 12: the $w1 engine pod dies; republish restages"
e1=$(k -n "$ns" get pods -l "$sel,constellation.replicix.com/node=$w1" -o jsonpath='{.items[0].metadata.name}')
uid=$(k -n "$ns" get pod "$e1" -o jsonpath='{.metadata.uid}')
k -n "$ns" delete pod "$e1" --grace-period=0 --force >/dev/null 2>&1
# kubelet republishes every staged volume of a requiresRepublish driver on
# its sync loop (about a minute); the first republish restages.
for _ in $(seq 90); do
    now=$(k -n "$ns" get pod "$e1" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
    if [ -n "$now" ] && [ "$now" != "$uid" ] \
        && [ "$(k -n "$ns" get pod "$e1" -o jsonpath='{.metadata.annotations.constellation\.dev/last-view-count}')" = 2 ]; then
        break
    fi
    sleep 2
done
[ "$(views "$w1")" = 2 ] || { echo "no restage onto a new $e1 (views: $(views "$w1"))"; exit 1; }
pod a4 "$w1" 'true' k3-a
in_pod a4 'sha256sum /data/k3-a/blob' | diff -q - /tmp/k3-blob.sha >/dev/null \
    || { echo "a new pod on $w1 cannot read pvc a after the restage"; exit 1; }
echo "   $e1 is back (new incarnation), both volumes restaged; a4 reads pvc a"

echo "== every pod gone: the volumes unstage"
k delete pod a1 a2 a3 a4 b1 --wait=true >/dev/null
fuse_mounts() { docker exec "$1" grep -c 'fuse.constellation' /proc/self/mountinfo || true; }
for _ in $(seq 60); do
    left=$(( $(fuse_mounts "$w1") + $(fuse_mounts "$w2") ))
    [ "$left" = 0 ] && break
    sleep 2
done
[ "$left" = 0 ] || { echo "$left FUSE mounts left on the workers after unstaging"; exit 1; }
[ "$(views "$w1")" = 0 ] && [ "$(views "$w2")" = 0 ] \
    || { echo "engine pods not annotated idle ($(views "$w1"), $(views "$w2"))"; exit 1; }
k delete pvc k3-a k3-b --wait=true >/dev/null
echo "== k3-smoke PASSED: pods read and wrote through PVs on $w1 and $w2; one engine pod per (pool, node); restage after an engine crash; clean unstage"
