#!/usr/bin/env bash
# Bring up the kind cluster plan 37's chunks test against: tests/csi/kind-config.yaml
# (two workers, /dev/fuse), the constellation-csi image loaded (never pushed),
# and optionally the Helm chart installed.
#
# Usage: tests/csi/kind-up.sh [--install]
#
# Knobs:
#   KIND_BIN          kind binary (default: `kind` on PATH)
#   KIND_CLUSTER      cluster name (default: constellation-csi; one cluster per host!)
#   KIND_CONFIG_DRAFT cluster config (default: tests/csi/kind-config.yaml)
#   KIND_NODE_IMAGE   --image for kind create (default: kind's own)
#   CSI_IMAGE         image to load (default: constellation-csi:dev; `make csi-image`)
#   CSI_NAMESPACE     install namespace (default: constellation-csi)
#   CSI_SIDECARS_PRELOAD  load the sidecar images from the host docker (multi-arch manifests often fail `kind load`; off by default, nodes pull them)
#                     (default 0).
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
kind="${KIND_BIN:-kind}"
name="${KIND_CLUSTER:-constellation-csi}"
config="${KIND_CONFIG_DRAFT:-$root/tests/csi/kind-config.yaml}"
image="${CSI_IMAGE:-constellation-csi:dev}"
ns="${CSI_NAMESPACE:-constellation-csi}"
chart="$root/deploy/helm/constellation-csi"

if "$kind" get clusters 2>/dev/null | grep -qx "$name"; then
    echo "== kind cluster $name already exists"
else
    args=(create cluster --name "$name" --config "$config" --wait 120s)
    [ -n "${KIND_NODE_IMAGE:-}" ] && args+=(--image "$KIND_NODE_IMAGE")
    "$kind" "${args[@]}"
fi

docker image inspect "$image" >/dev/null 2>&1 || {
    echo "image $image not found; run \`make csi-image\` first" >&2
    exit 1
}
"$kind" load docker-image "$image" --name "$name"

if [ "${CSI_SIDECARS_PRELOAD:-0}" = 1 ]; then
    for img in $(helm template x "$chart" | grep -o 'registry.k8s.io/[^"[:space:]]*' | sort -u); do
        docker image inspect "$img" >/dev/null 2>&1 || docker pull -q "$img"
        "$kind" load docker-image "$img" --name "$name"
    done
fi

if [ "${1:-}" = "--install" ]; then
    # repo[:tag]; a ':' before the last '/' is a registry port, not a tag.
    repo="$image" tag=latest
    if [[ "${image##*/}" == *:* ]]; then
        repo="${image%:*}" tag="${image##*:}"
    fi
    kubectl --context "kind-$name" create namespace "$ns" --dry-run=client -o yaml \
        | kubectl --context "kind-$name" apply -f -
    kubectl --context "kind-$name" label namespace "$ns" \
        pod-security.kubernetes.io/enforce=privileged --overwrite
    # The chart's csi-snapshotter needs them (plan 37 §14, K4).
    KUBE_CONTEXT="kind-$name" "$root/tests/csi/snapshot-crds.sh"
    helm --kube-context "kind-$name" upgrade --install constellation-csi "$chart" \
        -n "$ns" --set image.repository="$repo" --set image.tag="$tag" \
        --wait --timeout 180s
fi
echo "== cluster $name ready (kubectl --context kind-$name)"
