#!/usr/bin/env bash
# Run harness scenarios once per FUSE transport (plan 38 §6).
#
# The point of the lane is that a transport-specific read-path regression
# shows up as a scenario failing on exactly one leg instead of a blanket
# failure. Two legs exist from plan 38 Z1 on:
#
#   dev-fuse  the `/dev/fuse` `writev` path Constellation has always used
#             and still defaults to; available everywhere.
#   auto      plan 38 §2.4's ladder. Takes FUSE-over-io_uring when the
#             binary carries the `io-uring` feature, the kernel is 6.14+
#             with `fuse.enable_uring=Y`, and the sandbox permits
#             `io_uring_setup(2)`; falls back to `/dev/fuse` otherwise.
#
# **The `auto` leg is not conditional, and never skips.** A host that
# cannot grant the ring runs it as a fallback leg, which is exactly the
# property the ladder promises and therefore worth a lane of its own: the
# same scenarios must pass when `auto` degrades. What the leg negotiated
# is printed by the `transport-detach-refused` scenario (and is in
# `node.status`'s per-mount `transport` field), so a leg that silently
# never reached the ring is visible rather than mistaken for coverage.
#
# To get the ring the binary must be built with the feature (the `make`
# targets do this):
#   cargo build --release --features constellation-frontend-fuse/io-uring \
#       -p constellation -p constellation-harness
#
# On a host whose kernel grants the ring (`fuse.enable_uring=Y`, io_uring
# not disabled by sysctl) a fallback is a failure, not coverage: the lane
# exports CONSTELLATION_FUSE_EXPECT_URING=1 and always runs
# `transport-detach-refused`, which then fails if `auto` negotiated
# `dev_fuse` (a binary built without the feature, a lost ladder rung).
# EXPECT_URING=0|1 overrides the detection.
#
# Usage:
#   tests/transport-matrix.sh                        # every leg, read-path scenarios
#   TRANSPORTS="dev-fuse" tests/transport-matrix.sh
#   SCENARIOS="cold-cache readahead" tests/transport-matrix.sh
#   SCENARIOS=all tests/transport-matrix.sh          # the FULL matrix, per leg
#   READ_CPU_GATE=1 tests/transport-matrix.sh        # also run the fio cost gate per leg
#
# Knobs: HARNESS_BIN, CONSTELLATION_BIN, HARNESS_SEED, RESULTS_DIR.
set -euo pipefail
cd "$(dirname "$0")/.."

TARGET="${CARGO_TARGET_DIR:-target}"
HARNESS="${HARNESS_BIN:-$TARGET/release/harness}"
# Absolute: the harness spawns the daemon with it, from a scenario's own
# temporary working directory.
export CONSTELLATION_BIN="${CONSTELLATION_BIN:-$PWD/$TARGET/release/constellation}"
SEED="${HARNESS_SEED:-42}"
RESULTS_DIR="${RESULTS_DIR:-$TARGET/transport-matrix}"

# Every transport a mount may be asked for. `uring`/`uring-zc` are not
# policies: the ladder negotiates them, `auto` is how one asks.
TRANSPORTS="${TRANSPORTS:-dev-fuse auto}"

# The read-path scenarios: the cache/prefetch/coop paths a transport
# change can break, plus the end-to-end and fault legs that read back
# what they wrote, plus the transport's own handover policy, the blocked-
# close regression of plan 38 Z1b (`s3-cut-one-node`) and Z2a's injected
# downgrades (the ring-only ones skip, naming why, where the host or build
# cannot grant the ring; `transport-seccomp-denied` runs everywhere). Deliberately
# not the full matrix — `SCENARIOS=all` (or `make harness-transport-matrix`)
# is the slower gate.
SCENARIOS="${SCENARIOS:-cold-cache readahead coop-cache-hit coop-exact-churn e2e-basic \
e2e-two-nodes poison-record-isolation s3-outage truncate-never-resurrects fio-latency \
s3-cut-one-node transport-detach-refused transport-seccomp-denied \
transport-refused-registration transport-enomem-ring transport-abort-while-armed}"
if [ "$SCENARIOS" = all ]; then SCENARIOS=""; fi

# Whether this host's kernel grants FUSE-over-io_uring to an unprivileged
# daemon (plan 38 §2.4's kernel rungs; the build rung is what is checked).
host_grants_uring() {
    [ "$(cat /sys/module/fuse/parameters/enable_uring 2>/dev/null)" = Y ] || return 1
    local disabled
    disabled=$(cat /proc/sys/kernel/io_uring_disabled 2>/dev/null || echo 0)
    [ "$disabled" = 0 ]
}
if [ -z "${EXPECT_URING:-}" ]; then
    if host_grants_uring; then EXPECT_URING=1; else EXPECT_URING=0; fi
fi
if [ "$EXPECT_URING" = 1 ]; then
    echo "== this host grants the ring: a fallback to dev_fuse fails the lane"
    export CONSTELLATION_FUSE_EXPECT_URING=1
    case " $SCENARIOS " in
        "  ") ;; # the full matrix includes it
        *" transport-detach-refused "*) ;;
        *) SCENARIOS="$SCENARIOS transport-detach-refused" ;;
    esac
fi

[ -x "$HARNESS" ] || { echo "FAIL: no harness binary at $HARNESS (cargo build --release)" >&2; exit 2; }
[ -x "$CONSTELLATION_BIN" ] || { echo "FAIL: no constellation binary at $CONSTELLATION_BIN" >&2; exit 2; }
mkdir -p "$RESULTS_DIR"

rc=0
for transport in $TRANSPORTS; do
    case "$transport" in
        dev-fuse|auto) ;;
        *) echo "FAIL: unknown transport $transport (expected dev-fuse or auto)" >&2; exit 2 ;;
    esac
    echo "== transport $transport: harness run ${SCENARIOS:-<full matrix>}"
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
