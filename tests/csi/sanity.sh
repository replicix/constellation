#!/usr/bin/env bash
# csi-sanity against constellation-csi's Identity and Controller services
# (plan 37 K1 + K2 gates): starts the controller role on one temp unix
# socket, backed by its in-process engine fake (--in-memory-backend, so the
# suite never touches S3 — plan 37 §12), and the node role on another, and
# runs csi-sanity's Identity and Controller spec groups. csi-sanity cleans
# up every Controller-test volume through the Node service too, which is
# why the node role runs at all; its staging/publishing RPCs are
# UNIMPLEMENTED until K3, so the Node group is not run yet — K3's sanity.sh
# drops the focus.
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
RUST_LOG="${RUST_LOG:-info}" "$bin" --node --node-id csi-sanity --endpoint "unix://$node_sock" >"$node_log" 2>&1 &
node_pid=$!
wait_for_socket "$sock" "$pid" "$log"
wait_for_socket "$node_sock" "$node_pid" "$node_log"

status=0
# Identity against the controller role on its own: the main run below talks
# Identity to whatever --csi.endpoint is, which is the node role.
echo "== csi-sanity --ginkgo.focus=Identity (controller role)"
"$sanity" --csi.endpoint="unix://$sock" --ginkgo.focus='Identity' --ginkgo.fail-on-empty \
    || status=$?
echo "== csi-sanity --ginkgo.focus='Identity|Controller' (node + controller roles)"
"$sanity" --csi.endpoint="unix://$node_sock" --csi.controllerendpoint="unix://$sock" \
    --csi.testvolumeparameters="$params" \
    --csi.mountdir="$work/mount" --csi.stagingdir="$work/staging" \
    --ginkgo.focus='Identity|Controller' --ginkgo.fail-on-empty ${CSI_SANITY_ARGS:-} || status=$?
if [ "$status" -ne 0 ]; then
    echo "== constellation-csi --controller log (tail)" >&2
    tail -n 50 "$log" >&2
    echo "== constellation-csi --node log (tail)" >&2
    tail -n 20 "$node_log" >&2
fi
exit "$status"
