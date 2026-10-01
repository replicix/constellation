#!/usr/bin/env bash
# Run the read-path harness scenarios once per FUSE transport (plan 38 §6).
#
# The point of the lane is that a transport-specific read-path regression
# shows up as a scenario failing on exactly one leg instead of a blanket
# failure. Today there is exactly one transport — `dev-fuse`, the
# `/dev/fuse` `writev` path Constellation has always used — so one leg
# runs and the lane is a skeleton. Plan 38's Z1 vendors the
# FUSE-over-io_uring transport and Z2 turns `TransportPolicy::Auto` on by
# default; from Z1 on, `TRANSPORTS="dev-fuse auto"` is the CI invocation
# and this script needs no change to gain the second leg.
#
# Until Z1, `CONSTELLATION_FUSE_TRANSPORT` is a name this lane sets and
# nothing in the daemon reads: the value is recorded in the per-leg
# results (and in `tests/read-cpu-gate.sh`'s JSON lines) so the two legs
# are distinguishable once they differ. Z1 makes the daemon honour it and
# refuse an unknown value.
#
# Usage:
#   tests/transport-matrix.sh                        # every leg, default scenarios
#   TRANSPORTS="dev-fuse" tests/transport-matrix.sh
#   SCENARIOS="cold-cache readahead" tests/transport-matrix.sh
#   READ_CPU_GATE=1 tests/transport-matrix.sh        # also run the fio cost gate per leg
#
# Knobs: HARNESS_BIN, CONSTELLATION_BIN, HARNESS_SEED, RESULTS_DIR.
set -euo pipefail
cd "$(dirname "$0")/.."

TARGET="${CARGO_TARGET_DIR:-target}"
HARNESS="${HARNESS_BIN:-$TARGET/release/harness}"
export CONSTELLATION_BIN="${CONSTELLATION_BIN:-$PWD/$TARGET/release/constellation}"
SEED="${HARNESS_SEED:-42}"
RESULTS_DIR="${RESULTS_DIR:-$TARGET/transport-matrix}"

# Every transport this build can negotiate. `auto` joins the list at Z1.
TRANSPORTS="${TRANSPORTS:-dev-fuse}"

# The read-path scenarios: the cache/prefetch/coop paths a transport
# change can break, plus the end-to-end and fault legs that read back
# what they wrote. Deliberately not the full matrix — `harness run` with
# no arguments is a separate, slower gate.
SCENARIOS="${SCENARIOS:-cold-cache readahead coop-cache-hit coop-exact-churn e2e-basic \
e2e-two-nodes poison-record-isolation s3-outage truncate-never-resurrects fio-latency}"

[ -x "$HARNESS" ] || { echo "FAIL: no harness binary at $HARNESS (cargo build --release)" >&2; exit 2; }
mkdir -p "$RESULTS_DIR"

rc=0
for transport in $TRANSPORTS; do
    case "$transport" in
        dev-fuse) ;;
        auto|uring|uring-zc)
            echo "== transport $transport: not implemented before plan 38 Z1; SKIP"
            continue
            ;;
        *) echo "FAIL: unknown transport $transport" >&2; exit 2 ;;
    esac
    echo "== transport $transport: harness run $SCENARIOS"
    # shellcheck disable=SC2086
    CONSTELLATION_FUSE_TRANSPORT="$transport" \
        "$HARNESS" run $SCENARIOS --seed "$SEED" \
        --results-json "$RESULTS_DIR/$transport.json" || rc=$?
    if [ "${READ_CPU_GATE:-0}" = 1 ]; then
        echo "== transport $transport: read-cpu gate"
        CONSTELLATION_FUSE_TRANSPORT="$transport" \
            READ_CPU_OUT="$RESULTS_DIR/$transport.read-cpu.jsonl" \
            tests/read-cpu-gate.sh || rc=$?
    fi
done
[ "$rc" -eq 0 ] && echo "TRANSPORT MATRIX PASSED (${TRANSPORTS})"
exit "$rc"
