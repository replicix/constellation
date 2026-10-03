#!/usr/bin/env bash
# Install the chart's snapshot prerequisites on a cluster (plan 37 §14,
# K4): the snapshot CRDs (VolumeSnapshotClass, VolumeSnapshotContent,
# VolumeSnapshot, and the group-snapshot ones the snapshot-controller
# watches) and the cluster-wide snapshot-controller, from
# kubernetes-csi/external-snapshotter at the tag the chart's csi-snapshotter
# sidecar is pinned to. Installed once per cluster, never by the chart:
# upstream's own guidance, since every snapshot-capable driver shares them.
#
# Idempotent (`kubectl apply`). Waits until the CRDs are established and the
# controller has rolled out.
#
# Usage: tests/csi/snapshot-crds.sh
#
# Knobs:
#   KUBECONFIG         the cluster (kubectl's usual lookup otherwise)
#   KUBE_CONTEXT       a kubeconfig context to use instead of the current one
#   SNAPSHOTTER_TAG    external-snapshotter tag (default: the chart's
#                      sidecars.snapshotter.tag, v8.6.0)
#   SNAPSHOTTER_BASE   where the manifests come from (default: GitHub raw at
#                      that tag; a local checkout's path works too)
set -euo pipefail

tag="${SNAPSHOTTER_TAG:-v8.6.0}"
base="${SNAPSHOTTER_BASE:-https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/$tag}"

k() { kubectl ${KUBE_CONTEXT:+--context "$KUBE_CONTEXT"} "$@"; }

fetch() {
    case "$base" in
        http://* | https://*) curl -fsSL --retry 3 "$base/$1" ;;
        *) cat "$base/$1" ;;
    esac
}

crds=(
    snapshot.storage.k8s.io_volumesnapshotclasses.yaml
    snapshot.storage.k8s.io_volumesnapshotcontents.yaml
    snapshot.storage.k8s.io_volumesnapshots.yaml
    groupsnapshot.storage.k8s.io_volumegroupsnapshotclasses.yaml
    groupsnapshot.storage.k8s.io_volumegroupsnapshotcontents.yaml
    groupsnapshot.storage.k8s.io_volumegroupsnapshots.yaml
)
echo "== snapshot CRDs (external-snapshotter $tag)"
for f in "${crds[@]}"; do
    fetch "client/config/crd/$f" | k apply -f -
done
k wait --for=condition=Established --timeout=120s \
    crd/volumesnapshotclasses.snapshot.storage.k8s.io \
    crd/volumesnapshotcontents.snapshot.storage.k8s.io \
    crd/volumesnapshots.snapshot.storage.k8s.io

echo "== snapshot-controller $tag (kube-system)"
fetch deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml | k apply -f -
# Upstream's manifest at a tag may still name the previous release's image:
# pin it to the tag asked for.
fetch deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml |
    sed -E "s#(registry.k8s.io/sig-storage/snapshot-controller):v[0-9.]+#\1:$tag#" |
    k apply -f -
k -n kube-system rollout status deploy/snapshot-controller --timeout=300s
echo "== snapshot prerequisites installed"
