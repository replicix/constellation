#!/usr/bin/env bash
# csi-sanity's Identity group against the *deployed* node plugin on a kind
# worker (plan 37 K1 gate): copies csi-sanity into the worker's container and
# runs it against the plugin's hostPath socket, the same file kubelet and the
# registrar use. Needs tests/csi/kind-up.sh --install first.
#
# Knobs: CSI_SANITY_BIN, KIND_CLUSTER (default constellation-csi), KIND_BIN.
set -euo pipefail
name="${KIND_CLUSTER:-constellation-csi}"
sanity="${CSI_SANITY_BIN:-csi-sanity}"
sanity="$(readlink -f "$(command -v "$sanity")")"
node="$(docker ps --format '{{.Names}}' | grep -x "$name-worker")"
sock="${CSI_PLUGIN_SOCK:-/var/lib/kubelet/plugins/constellation.csi.replicix.com/csi.sock}"
# /tmp is a tmpfs in the kind node, which `docker cp` cannot write to.
docker cp "$sanity" "$node:/usr/local/bin/csi-sanity"
docker exec "$node" /usr/local/bin/csi-sanity --csi.endpoint="unix://$sock" \
    --ginkgo.focus='Identity' --ginkgo.fail-on-empty
