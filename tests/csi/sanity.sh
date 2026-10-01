#!/usr/bin/env bash
# csi-sanity against constellation-csi's Identity service (plan 37 K1 gate):
# starts the controller binary on a temp unix socket and runs csi-sanity's
# Identity spec group against it. Controller/Node RPCs (everything past
# Identity) are UNIMPLEMENTED stubs until K2/K3, so only Identity is run —
# a later milestone's sanity.sh drops this --ginkgo.focus restriction.
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
sock="$work/csi.sock"
log="$work/constellation-csi.log"
pid=""

cleanup() {
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
    [ -n "$pid" ] && wait "$pid" 2>/dev/null || true
    rm -rf "$work"
}
trap cleanup EXIT

echo "== starting constellation-csi --controller on $sock"
RUST_LOG="${RUST_LOG:-info}" "$bin" --controller --endpoint "unix://$sock" >"$log" 2>&1 &
pid=$!

for _ in $(seq 1 50); do
    [ -S "$sock" ] && break
    kill -0 "$pid" 2>/dev/null || {
        echo "constellation-csi exited early; log:" >&2
        cat "$log" >&2
        exit 1
    }
    sleep 0.1
done
[ -S "$sock" ] || {
    echo "constellation-csi never created $sock" >&2
    cat "$log" >&2
    exit 1
}

echo "== csi-sanity --ginkgo.focus=Identity"
"$sanity" --csi.endpoint="unix://$sock" --ginkgo.focus='Identity' --ginkgo.fail-on-empty
