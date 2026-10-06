#!/usr/bin/env bash
# csi-sanity against constellation-csi's Identity, Controller and Node
# services (plan 37 K1 + K2 + K3 gates), all on their in-process fakes so
# the suite never touches S3 or mount(2) (plan 37 §12): the controller role
# on one temp unix socket (--in-memory-backend: an in-memory engine), the
# node role on another (--in-memory-backend: in-memory engine pods, and
# mounts that are recorded rather than made — the directories are real).
# The two processes share no engine, so the node's fake takes every volume
# the controller's names to exist. The same Node group against the real
# node plugin, real FUSE mounts and real engine pods is
# tests/csi/sanity-kind.sh.
#
# Usage: tests/csi/sanity.sh
#
# Knobs:
#   CONSTELLATION_CSI_BIN  constellation-csi binary (default: build/find it,
#                          same convention as tests/smoke.sh's harness lookup)
#   CSI_SANITY_BIN         the kubernetes-csi/csi-test sanity binary
#                          (default: `csi-sanity` on PATH)
set -euo pipefail

target="${CARGO_TARGET_DIR:-target}"
bin="${CONSTELLATION_CSI_BIN:-}"
if [ -z "$bin" ]; then
    if [ -x "$target/debug/constellation-csi" ]; then
        bin="$target/debug/constellation-csi"
    elif [ -x "$target/release/constellation-csi" ]; then
        bin="$target/release/constellation-csi"
    else
        echo "== building constellation-csi (no binary found)"
        (cd "$(dirname "$0")/../.." && cargo build -q -p constellation-csi)
        bin="$target/debug/constellation-csi"
    fi
fi
sanity="${CSI_SANITY_BIN:-csi-sanity}"
if ! command -v "$sanity" >/dev/null 2>&1 && [ ! -x "$sanity" ]; then
    echo "csi-sanity not found (CSI_SANITY_BIN=$sanity); install kubernetes-csi/csi-test's cmd/csi-sanity and point CSI_SANITY_BIN at it, or put it on PATH" >&2
    exit 1
fi

work=$(mktemp -d /tmp/constellation-csi-sanity.XXXXXX)
sock="$work/controller.sock"
node_sock="$work/node.sock"
log="$work/constellation-csi.log"
node_log="$work/constellation-csi-node.log"
params="$work/volume-parameters.yaml"
secrets="$work/secrets.yaml"
pid=""
node_pid=""

# The StorageClass parameters csi-sanity's CreateVolume calls carry: a
# pool class needs at least a bucket (plan 37 §6). Sharded, so the shard
# routing is exercised too.
cat >"$params" <<'YAML'
bucket: csi-sanity
prefix: constellation-csi/sanity
layout: pool
shards: "2"
YAML

# The Secrets the sidecars and kubelet would pass. The class is
# `static-ephemeral` (the default), so the in-memory engine pods wait for
# their credentials (`fs.unlock`) as real ones do (37-k6a), and a
# NodeStageVolume without a secret is refused `UNAVAILABLE`.
creds='{aws_access_key_id: test, aws_secret_access_key: test}'
cat >"$secrets" <<YAML
CreateVolumeSecret: $creds
DeleteVolumeSecret: $creds
ControllerExpandVolumeSecret: $creds
ControllerValidateVolumeCapabilitiesSecret: $creds
NodeStageVolumeSecret: $creds
CreateSnapshotSecret: $creds
DeleteSnapshotSecret: $creds
ListSnapshotsSecret: $creds
YAML

cleanup() {
    for p in $pid $node_pid; do
        kill "$p" 2>/dev/null || true
        wait "$p" 2>/dev/null || true
    done
    rm -rf "$work"
}
trap cleanup EXIT

# wait_for_socket SOCK PID LOG
wait_for_socket() {
    for _ in $(seq 1 50); do
        [ -S "$1" ] && return 0
        kill -0 "$2" 2>/dev/null || {
            echo "constellation-csi exited early; log:" >&2
            cat "$3" >&2
            exit 1
        }
        sleep 0.1
    done
    echo "constellation-csi never created $1" >&2
    cat "$3" >&2
    exit 1
}

echo "== starting constellation-csi --controller on $sock"
RUST_LOG="${RUST_LOG:-info}" "$bin" --controller --in-memory-backend --endpoint "unix://$sock" >"$log" 2>&1 &
pid=$!
echo "== starting constellation-csi --node on $node_sock"
RUST_LOG="${RUST_LOG:-info}" "$bin" --node --node-id csi-sanity --in-memory-backend \
    --host-root "$work/host" --endpoint "unix://$node_sock" >"$node_log" 2>&1 &
node_pid=$!
wait_for_socket "$sock" "$pid" "$log"
wait_for_socket "$node_sock" "$node_pid" "$node_log"

status=0
# Identity against the controller role on its own: the main run below talks
# Identity to whatever --csi.endpoint is, which is the node role.
echo "== csi-sanity --ginkgo.focus=Identity (controller role)"
"$sanity" --csi.endpoint="unix://$sock" --ginkgo.focus='Identity' --ginkgo.fail-on-empty \
    || status=$?
echo "== csi-sanity --ginkgo.focus='Identity|Controller|Node' (node + controller roles)"
"$sanity" --csi.endpoint="unix://$node_sock" --csi.controllerendpoint="unix://$sock" \
    --csi.testvolumeparameters="$params" --csi.secrets="$secrets" \
    --csi.mountdir="$work/mount" --csi.stagingdir="$work/staging" \
    --ginkgo.focus='Identity|Controller|Node' --ginkgo.fail-on-empty ${CSI_SANITY_ARGS:-} || status=$?
if [ "$status" -ne 0 ]; then
    echo "== constellation-csi --controller log (tail)" >&2
    tail -n 50 "$log" >&2
    echo "== constellation-csi --node log (tail)" >&2
    tail -n 20 "$node_log" >&2
fi
exit "$status"
