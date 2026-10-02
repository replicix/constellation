#!/usr/bin/env bash
# Plan 37 K2 gate on kind: a pool StorageClass provisions against ONE
# Constellation filesystem through ONE controller-owned engine pod.
#
#   1. kind cluster (tests/csi/kind-up.sh) + the chart, provisioner and
#      resizer sidecars on;
#   2. a private floci S3 on the `kind` docker network (real S3 API, so the
#      engine pod's `fs.create{bucket, prefix}` is real);
#   3. a pool StorageClass + its credentials Secret; PVC a → Bound, PVC b →
#      Bound;
#   4. asserts: both PVs name the same filesystem uuid, which is the uuid in
#      the pool's meta.json (read from S3 inside the engine pod), the engine
#      pod's own label, and what `fs.create` answers; exactly one
#      controller-owned engine pod exists; it is unprivileged and not
#      nodeName-pinned; each volume's subtree quota is its PVC size;
#   5. `kubectl patch` grows PVC a → the PV, the PVC and the subtree quota
#      follow;
#   6. the controller ServiceAccount's pod access is held to its engine
#      pods by the chart's ValidatingAdmissionPolicy: exec into the engine
#      pod is allowed, exec into a node-plugin pod and a privileged pod are
#      denied;
#   7. a lost engine pod the controller has no spec for (controller pods
#      restarted, then the engine pod deleted) is rebuilt from the PV and
#      its StorageClass by the next expansion;
#   8. deleting PVC b deletes its PV and moves its subtree out of /volumes.
#
# Usage: tests/csi/k2-smoke.sh
# Knobs:
#   KIND_BIN, KIND_CLUSTER (default k2-smoke), KIND_CONFIG_DRAFT,
#   KIND_NODE_IMAGE      as for kind-up.sh
#   CSI_IMAGE            image to load (default constellation-csi:dev;
#                        built with `make csi-image` unless K2_SKIP_BUILD=1)
#   K2_KEEP=1            leave the cluster and the S3 container running
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
kind="${KIND_BIN:-kind}"
export KIND_CLUSTER="${KIND_CLUSTER:-k2-smoke}"
export CSI_IMAGE="${CSI_IMAGE:-constellation-csi:dev}"
export CSI_NAMESPACE="${CSI_NAMESPACE:-constellation-csi}"
ctx="kind-$KIND_CLUSTER"
ns="$CSI_NAMESPACE"
s3c="$KIND_CLUSTER-floci"
bucket=k2-smoke
k() { kubectl --context "$ctx" "$@"; }

cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        echo "== FAILED; diagnostics"
        k -n "$ns" get pods -o wide 2>/dev/null || true
        k get pvc,pv 2>/dev/null || true
        k -n "$ns" logs deploy/constellation-csi-controller -c constellation-csi --tail=60 2>/dev/null || true
        k -n "$ns" logs -l app.kubernetes.io/component=engine -c engine --tail=40 2>/dev/null || true
    fi
    if [ "${K2_KEEP:-0}" != 1 ]; then
        "$kind" delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
        docker rm -f "$s3c" >/dev/null 2>&1 || true
    fi
    exit "$status"
}
trap cleanup EXIT

if [ "${K2_SKIP_BUILD:-0}" != 1 ]; then
    echo "== building $CSI_IMAGE"
    make -C "$root" csi-image CSI_IMAGE="$CSI_IMAGE"
fi

echo "== kind cluster $KIND_CLUSTER + chart"
"$root/tests/csi/kind-up.sh" --install

echo "== S3 (floci) on the kind network"
docker rm -f "$s3c" >/dev/null 2>&1 || true
docker run -d --name "$s3c" --network kind -e FLOCI_STORAGE_MODE=memory \
    floci/floci:1.7.0-compat >/dev/null
for _ in $(seq 60); do
    docker exec "$s3c" curl -sf http://localhost:4566/_floci/health >/dev/null 2>&1 && break
    sleep 1
done
docker exec -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_DEFAULT_REGION=us-east-1 \
    "$s3c" aws --endpoint-url http://localhost:4566 s3 mb "s3://$bucket" >/dev/null
s3ip=$(docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "$s3c")
endpoint="http://$s3ip:4566"
echo "   endpoint $endpoint, bucket $bucket"

echo "== pool StorageClass, credentials, two PVCs"
k -n "$ns" create secret generic constellation-s3-creds \
    --from-literal=aws_access_key_id=test --from-literal=aws_secret_access_key=test \
    --dry-run=client -o yaml | k apply -f -
k apply -f - <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: k2-pool
provisioner: csi.constellation.dev
parameters:
  bucket: "$bucket"
  prefix: "constellation-csi/k2-pool"
  endpoint: "$endpoint"
  region: "us-east-1"
  layout: "pool"
  chunkSize: "1MiB"
  csi.storage.k8s.io/provisioner-secret-name: constellation-s3-creds
  csi.storage.k8s.io/provisioner-secret-namespace: "$ns"
  csi.storage.k8s.io/controller-expand-secret-name: constellation-s3-creds
  csi.storage.k8s.io/controller-expand-secret-namespace: "$ns"
reclaimPolicy: Delete
allowVolumeExpansion: true
volumeBindingMode: Immediate
EOF
pvc() {
    k apply -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: $1, namespace: default}
spec:
  accessModes: [ReadWriteMany]
  storageClassName: k2-pool
  resources: {requests: {storage: $2}}
EOF
}
bound() {
    k wait --for=jsonpath='{.status.phase}'=Bound "pvc/$1" --timeout=300s >/dev/null
    echo "   pvc/$1 Bound ($(k get pvc "$1" -o jsonpath='{.spec.volumeName}'))"
}
pvc k2-a 1Gi
bound k2-a
pvc k2-b 2Gi
bound k2-b

handle() { k get pv "$(k get pvc "$1" -o jsonpath='{.spec.volumeName}')" -o jsonpath='{.spec.csi.volumeHandle}'; }
ha=$(handle k2-a)
hb=$(handle k2-b)
echo "   volumeHandles: $ha, $hb"
uuid_a=$(cut -d/ -f4 <<<"$ha")
uuid_b=$(cut -d/ -f4 <<<"$hb")
[ "$uuid_a" = "$uuid_b" ] || { echo "PVCs landed on different filesystems: $uuid_a vs $uuid_b"; exit 1; }

echo "== exactly one controller-owned engine pod"
sel=app.kubernetes.io/component=engine,constellation.dev/owner=controller
pods=$(k -n "$ns" get pods -l "$sel" -o name)
# `grep -c .`, not `wc -l`: an empty list is 0 lines, not 1.
[ "$(grep -c . <<<"$pods")" = 1 ] || { echo "expected one engine pod, got: ${pods:-none}"; exit 1; }
pod=${pods#pod/}
label=$(k -n "$ns" get pod "$pod" -o jsonpath='{.metadata.labels.constellation\.dev/fs-uuid}')
node=$(k -n "$ns" get pod "$pod" -o jsonpath='{.spec.nodeName}')
echo "   $pod on $node, fs-uuid label $label"
[ "$label" = "$uuid_a" ] || { echo "engine pod label $label != volume uuid $uuid_a"; exit 1; }
# Unprivileged and scheduler-placed (§7).
spec=$(k -n "$ns" get pod "$pod" -o json)
jq -e '.spec.containers[0].securityContext.allowPrivilegeEscalation == false
       and .spec.containers[0].securityContext.privileged != true
       and .spec.securityContext.runAsNonRoot == true
       and (.spec.containers[0].securityContext.capabilities.drop | index("ALL"))' \
    <<<"$spec" >/dev/null || { echo "engine pod is not unprivileged"; exit 1; }
jq -e '.spec.nodeName != null and (.metadata.ownerReferences[0].kind == "Deployment")' \
    <<<"$spec" >/dev/null || { echo "engine pod not scheduled / not owned by the controller Deployment"; exit 1; }

in_pod() { k -n "$ns" exec "$pod" -c engine -- /usr/local/bin/constellation "$@"; }
s3url="s3://$bucket/constellation-csi/k2-pool"
meta_uuid=$(in_pod status --s3 "$s3url" 2>/dev/null | jq -r .uuid)
echo "   meta.json uuid at $s3url: $meta_uuid"
[ "$meta_uuid" = "$uuid_a" ] || { echo "meta.json uuid $meta_uuid != volume uuid $uuid_a"; exit 1; }

quota() {
    in_pod quota get pool --state-dir /var/lib/constellation/state --subtree "/volumes/$1" 2>/dev/null
}
pv_a=$(k get pvc k2-a -o jsonpath='{.spec.volumeName}')
pv_b=$(k get pvc k2-b -o jsonpath='{.spec.volumeName}')
echo "   /volumes/$pv_a: $(quota "$pv_a")"
echo "   /volumes/$pv_b: $(quota "$pv_b")"
quota "$pv_a" | grep -q "/ 1073741824 bytes" || { echo "pvc a quota is not 1Gi"; exit 1; }
quota "$pv_b" | grep -q "/ 2147483648 bytes" || { echo "pvc b quota is not 2Gi"; exit 1; }

echo "== expand pvc a to 3Gi"
k patch pvc k2-a --type merge -p '{"spec":{"resources":{"requests":{"storage":"3Gi"}}}}' >/dev/null
k wait --for=jsonpath='{.status.capacity.storage}'=3Gi pvc/k2-a --timeout=180s >/dev/null
echo "   pv $(k get pv "$pv_a" -o jsonpath='{.spec.capacity.storage}'), pvc $(k get pvc k2-a -o jsonpath='{.status.capacity.storage}')"
echo "   /volumes/$pv_a: $(quota "$pv_a")"
quota "$pv_a" | grep -q "/ 3221225472 bytes" || { echo "quota did not follow the expansion"; exit 1; }

echo "== the controller ServiceAccount reaches only its engine pods"
as_ctl=(--as "system:serviceaccount:$ns:constellation-csi-controller")
k "${as_ctl[@]}" -n "$ns" exec "$pod" -c engine -- /usr/local/bin/constellation control-relay \
    --ping --socket /run/constellation-csi/control.sock >/dev/null 2>&1 \
    || { echo "the controller SA cannot exec into its engine pod"; exit 1; }
node_pod=$(k -n "$ns" get pods -l app.kubernetes.io/component=node -o name | head -1)
if out=$(k "${as_ctl[@]}" -n "$ns" exec "${node_pod#pod/}" -c constellation-csi -- true 2>&1); then
    echo "the controller SA could exec into the node plugin $node_pod"; exit 1
fi
grep -q 'constellation-csi-controller-pods' <<<"$out" \
    || { echo "exec refused, but not by the policy: $out"; exit 1; }
echo "   exec into $node_pod: denied by the policy"
if out=$(k "${as_ctl[@]}" -n "$ns" run constellation-engine-evil-controller --restart=Never \
    --image="$CSI_IMAGE" -l app.kubernetes.io/component=engine,constellation.dev/owner=controller \
    --overrides='{"spec":{"hostPID":true,"containers":[{"name":"x","image":"'"$CSI_IMAGE"'","securityContext":{"privileged":true}}]}}' \
    2>&1); then
    echo "the controller SA could create a privileged pod"; exit 1
fi
grep -q 'constellation-csi-controller-pods' <<<"$out" \
    || { echo "pod refused, but not by the policy: $out"; exit 1; }
echo "   privileged engine-named pod: denied by the policy"

echo "== a lost engine pod is rebuilt from its PV and StorageClass"
k -n "$ns" delete pods -l app.kubernetes.io/component=controller --wait=true >/dev/null
k -n "$ns" rollout status deploy/constellation-csi-controller --timeout=180s >/dev/null
k -n "$ns" delete pod "$pod" --wait=true >/dev/null
k patch pvc k2-a --type merge -p '{"spec":{"resources":{"requests":{"storage":"4Gi"}}}}' >/dev/null
k wait --for=jsonpath='{.status.capacity.storage}'=4Gi pvc/k2-a --timeout=300s >/dev/null
k -n "$ns" wait --for=condition=Ready "pod/$pod" --timeout=60s >/dev/null
[ "$(k -n "$ns" get pods -l "$sel" -o name | grep -c .)" = 1 ] || { echo "rebuild made a second pod"; exit 1; }
relabel=$(k -n "$ns" get pod "$pod" -o jsonpath='{.metadata.labels.constellation\.dev/fs-uuid}')
[ "$relabel" = "$uuid_a" ] || { echo "rebuilt pod serves $relabel, not $uuid_a"; exit 1; }
echo "   /volumes/$pv_a: $(quota "$pv_a")"
quota "$pv_a" | grep -q "/ 4294967296 bytes" || { echo "quota did not follow the expansion"; exit 1; }
echo "   $pod rebuilt (same name, same filesystem) and pvc a expanded to 4Gi"

echo "== delete pvc b"
k delete pvc k2-b --wait=true >/dev/null
k wait --for=delete "pv/$pv_b" --timeout=180s >/dev/null 2>&1 || true
if k get pv "$pv_b" >/dev/null 2>&1; then echo "pv $pv_b was not deleted"; exit 1; fi
if quota "$pv_b" >/dev/null; then echo "/volumes/$pv_b still exists"; exit 1; fi
echo "   pv $pv_b deleted; /volumes/$pv_b moved to /.trash"
[ "$(k -n "$ns" get pods -l "$sel" -o name | grep -c .)" = 1 ] || { echo "engine pod count changed"; exit 1; }

echo "== k2-smoke PASSED: 2 PVCs Bound on filesystem $uuid_a via $pod; expansion, pod-access policy, lost-pod rebuild and delete OK"
