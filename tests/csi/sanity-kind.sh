#!/usr/bin/env bash
# csi-sanity's Node group against the *real* node plugin on kind (plan 37 K3
# gate): real FUSE staging mounts, real node-owned engine pods, real S3 —
# not the in-process fakes tests/csi/sanity.sh runs on.
#
#   1. kind cluster (tests/csi/kind-up.sh) + the chart;
#   2. a private floci S3 on the `kind` network, its credentials as the
#      sanity secrets of every RPC that takes one;
#   3. picks a worker that runs a controller replica, and runs csi-sanity
#      *inside that worker's node container*: its Node endpoint is the node
#      plugin's hostPath socket (the file kubelet uses), its Controller
#      endpoint that controller pod's emptyDir socket (the same node's
#      filesystem), and its staging/mount directories live under the
#      kubelet plugin directory, which the node plugin mounts at the same
#      path (Bidirectional);
#   4. asserts the node-owned engine pod it staged through is pinned to that
#      worker and unprivileged.
#
# Usage: tests/csi/sanity-kind.sh
# Knobs:
#   KIND_BIN, KIND_CLUSTER (default sanity-kind), KIND_CONFIG_DRAFT,
#   KIND_NODE_IMAGE      as for kind-up.sh
#   CSI_IMAGE            image to load (default constellation-csi:dev; built
#                        with `make csi-image` unless CSI_SKIP_BUILD=1)
#   CSI_SANITY_BIN       csi-sanity (default: on PATH)
#   CSI_SANITY_FOCUS     --ginkgo.focus (default Node)
#   CSI_KEEP=1           leave the cluster and the S3 container running
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/csi/lib.sh"
kind="${KIND_BIN:-kind}"
export KIND_CLUSTER="${KIND_CLUSTER:-sanity-kind}"
export CSI_IMAGE="${CSI_IMAGE:-constellation-csi:dev}"
export CSI_NAMESPACE="${CSI_NAMESPACE:-constellation-csi}"
ctx="kind-$KIND_CLUSTER"
ns="$CSI_NAMESPACE"
s3c="$KIND_CLUSTER-floci"
bucket=sanity-kind
focus="${CSI_SANITY_FOCUS:-Node}"
sanity="${CSI_SANITY_BIN:-csi-sanity}"
sanity="$(readlink -f "$(command -v "$sanity")")"
k() { kubectl --context "$ctx" "$@"; }

cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        echo "== FAILED; diagnostics"
        k -n "$ns" get pods -o wide 2>/dev/null || true
        k -n "$ns" logs -l app.kubernetes.io/component=node -c constellation-csi --tail=80 \
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

# The worker the run happens on: one with a controller replica, whose
# emptyDir socket is then on that worker's filesystem.
k -n "$ns" rollout status deploy/constellation-csi-controller --timeout=180s >/dev/null
k -n "$ns" rollout status ds/constellation-csi-node --timeout=180s >/dev/null
read -r ctl_pod ctl_uid worker < <(k -n "$ns" get pods -l app.kubernetes.io/component=controller \
    -o jsonpath='{range .items[*]}{.metadata.name} {.metadata.uid} {.spec.nodeName}{"\n"}{end}' \
    | grep -v ' control-plane$' | head -1)
[ -n "${worker:-}" ] || { echo "no controller replica on a worker"; exit 1; }
node_ctr="$worker"
plugin=/var/lib/kubelet/plugins/constellation.csi.replicix.com
# The emptyDir's own path is longer than a unix socket address may be
# (108 bytes): reach it through a short symlink to its directory.
ctl_dir="/var/lib/kubelet/pods/$ctl_uid/volumes/kubernetes.io~empty-dir/socket-dir"
ctl_sock=/run/sanity-controller/csi.sock
docker exec "$node_ctr" ln -sfn "$ctl_dir" /run/sanity-controller
docker exec "$node_ctr" test -S "$plugin/csi.sock"
docker exec "$node_ctr" test -S "$ctl_sock"
echo "   node $worker: node plugin $plugin/csi.sock, controller $ctl_pod"

work=$(mktemp -d /tmp/sanity-kind.XXXXXX)
cat >"$work/params.yaml" <<EOF
bucket: "$bucket"
prefix: "constellation-csi/sanity"
endpoint: "$endpoint"
region: "us-east-1"
layout: "pool"
chunkSize: "1MiB"
EOF
creds='{aws_access_key_id: test, aws_secret_access_key: test}'
cat >"$work/secrets.yaml" <<EOF
CreateVolumeSecret: $creds
DeleteVolumeSecret: $creds
ControllerExpandVolumeSecret: $creds
ControllerValidateVolumeCapabilitiesSecret: $creds
NodeStageVolumeSecret: $creds
EOF
# /tmp is a tmpfs in the kind node, which `docker cp` cannot write to.
docker exec "$node_ctr" mkdir -p /usr/local/share/sanity "$plugin/sanity"
docker cp "$sanity" "$node_ctr:/usr/local/bin/csi-sanity"
docker cp "$work/params.yaml" "$node_ctr:/usr/local/share/sanity/params.yaml"
docker cp "$work/secrets.yaml" "$node_ctr:/usr/local/share/sanity/secrets.yaml"
rm -rf "$work"

echo "== csi-sanity --ginkgo.focus='$focus' (node plugin on $worker, real mounts)"
docker exec "$node_ctr" /usr/local/bin/csi-sanity \
    --csi.endpoint="unix://$plugin/csi.sock" \
    --csi.controllerendpoint="unix://$ctl_sock" \
    --csi.testvolumeparameters=/usr/local/share/sanity/params.yaml \
    --csi.secrets=/usr/local/share/sanity/secrets.yaml \
    --csi.stagingdir="$plugin/sanity/staging" \
    --csi.mountdir="$plugin/sanity/mount" \
    --csi.testvolumesize=$((64 << 20)) --csi.testvolumeexpandsize=$((128 << 20)) \
    --ginkgo.focus="$focus" --ginkgo.fail-on-empty --ginkgo.no-color --ginkgo.v

echo "== the engine pod csi-sanity staged through"
sel=app.kubernetes.io/component=engine,constellation.replicix.com/owner=node
k -n "$ns" get pods -l "$sel" -o wide
pods=$(k -n "$ns" get pods -l "$sel" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
[ "$(grep -c . <<<"$pods")" = 1 ] || { echo "expected one node-owned engine pod, got: ${pods:-none}"; exit 1; }
spec=$(k -n "$ns" get pod "$pods" -o json)
jq -e --arg n "$worker" '.spec.nodeName == $n
       and .spec.containers[0].securityContext.privileged != true
       and .spec.securityContext.runAsNonRoot == true
       and .metadata.ownerReferences[0].kind == "DaemonSet"
       and .metadata.annotations["constellation.replicix.com/last-view-count"] == "0"' \
    <<<"$spec" >/dev/null || { echo "the node engine pod is not pinned/unprivileged/owned/idle: $spec"; exit 1; }
if docker exec "$node_ctr" grep -q 'fuse.constellation' /proc/self/mountinfo; then
    echo "a staging mount outlived csi-sanity's cleanup:"
    docker exec "$node_ctr" grep 'fuse.constellation' /proc/self/mountinfo
    exit 1
fi
echo "== sanity-kind PASSED: csi-sanity '$focus' against the node plugin on $worker ($pods)"
