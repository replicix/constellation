#!/usr/bin/env bash
# Run harness scenarios once per FUSE transport (plan 38 §6).
#
# The point of the lane is that a transport-specific read-path regression
# shows up as a scenario failing on exactly one leg instead of a blanket
# failure. Three legs exist from plan 38 Z2c on:
#
#   dev-fuse  the `/dev/fuse` `writev` path; available everywhere.
#   auto      plan 38 §2.4's ladder, the shipped default for plain mounts.
#             Takes FUSE-over-io_uring when the binary carries the
#             `io-uring` feature (every Linux build of `constellation`
#             since Z2c), the kernel is 6.14+ with `fuse.enable_uring=Y`,
#             and the sandbox permits `io_uring_setup(2)`; falls back to
#             `/dev/fuse` otherwise. A mount with cluster locks (the
#             default with P2P, so most harness mounts) takes it too, at
#             queue depth 32 (decided 2026-10-05; contended blocking lock
#             waits past a queue's budget get ENOLCK): this leg is what the
#             default actually ships.
#   uring     the same ladder as `auto` since 2026-10-05 (before, the
#             opt-in that put cluster-lock mounts on the ring); kept as a
#             leg so the spelling stays exercised.
#
# **No leg skips.** A host that cannot grant the ring runs `auto` and
# `uring` as fallback legs, which is exactly the property the ladder
# promises: the same scenarios must pass when it degrades. Each leg ends
# with a census — how many of its mounts got which transport, and why the
# others fell back (`CONSTELLATION_HARNESS_TRANSPORT_CENSUS`, from each
# daemon's per-mount `FUSE transport` record) — so a leg that never
# reached the ring is visible rather than mistaken for coverage.
#
# On a host whose kernel grants the ring (`fuse.enable_uring=Y`, io_uring
# not disabled by sysctl) a fallback is a failure, not coverage: the lane
# exports CONSTELLATION_FUSE_EXPECT_URING=1 and always runs
# `transport-detach-refused`, which then fails if its mount (which asks
# for `uring` itself) negotiated `dev_fuse`. EXPECT_URING=0|1 overrides the
# detection. The `auto` leg needs `fuse.enable_uring=Y` to be anything but
# a second fallback leg; see docs/how-to-guides/development/TESTING.md
# for running it in a KVM guest.
#
# Usage:
#   tests/transport-matrix.sh                        # every leg, read-path scenarios
#   TRANSPORTS="dev-fuse" tests/transport-matrix.sh
#   SCENARIOS="cold-cache readahead" tests/transport-matrix.sh
#   SCENARIOS=all tests/transport-matrix.sh          # the FULL matrix, per leg
#   SCENARIOS=all HARNESS_ARGS="--shard 1/2" tests/transport-matrix.sh
#   READ_CPU_GATE=1 tests/transport-matrix.sh        # also run the fio cost gate per leg
#   READ_CPU_GATE=1 READ_CPU_BASELINE=b.json ...     # ... each leg against b-<leg>.json
#
# Knobs: HARNESS_BIN, CONSTELLATION_BIN, HARNESS_SEED, HARNESS_ARGS, RESULTS_DIR.
set -euo pipefail
cd "$(dirname "$0")/.."

TARGET="${CARGO_TARGET_DIR:-target}"
HARNESS="${HARNESS_BIN:-$TARGET/release/harness}"
# Absolute: the harness spawns the daemon with it, from a scenario's own
# temporary working directory.
export CONSTELLATION_BIN="${CONSTELLATION_BIN:-$PWD/$TARGET/release/constellation}"
SEED="${HARNESS_SEED:-42}"
RESULTS_DIR="${RESULTS_DIR:-$TARGET/transport-matrix}"
# Absolute, once: the census path goes to every daemon, which writes it
# from its own working directory.
case "$RESULTS_DIR" in
    /*) ;;
    *) RESULTS_DIR="$PWD/$RESULTS_DIR" ;;
esac

# Every policy a mount may be asked for (`--fuse-transport`). `uring_zc`
# is not one: the ladder negotiates it.
TRANSPORTS="${TRANSPORTS:-dev-fuse auto uring}"

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
transport-refused-registration transport-enomem-ring transport-abort-while-armed \
transport-cluster-locks-auto transport-lock-wait-budget}"
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
        dev-fuse|auto|uring) ;;
        *) echo "FAIL: unknown transport $transport (expected dev-fuse, auto or uring)" >&2; exit 2 ;;
    esac
    echo "== transport $transport: harness run ${SCENARIOS:-<full matrix>} ${HARNESS_ARGS:-}"
    census="$RESULTS_DIR/$transport.census.tsv"
    : > "$census"
    # shellcheck disable=SC2086
    CONSTELLATION_FUSE_TRANSPORT="$transport" \
        CONSTELLATION_HARNESS_TRANSPORT_CENSUS="$census" \
        "$HARNESS" run $SCENARIOS ${HARNESS_ARGS:-} --seed "$SEED" \
        --results-json "$RESULTS_DIR/$transport.json" || rc=$?
    # Columns: scenario, client, transport, fallback reason ("-": none).
    echo "== transport $transport: census of $(wc -l < "$census") mounts (transport, reason)"
    cut -f3,4 "$census" | sort | uniq -c | sort -rn | sed 's/^/   /'
    if [ "${READ_CPU_GATE:-0}" = 1 ]; then
        echo "== transport $transport: read-cpu gate"
        # A leg is compared with its own baseline: with READ_CPU_BASELINE
        # set, `<name>-<leg>.json` (created, i.e. blessed, by the leg's
        # first run), never another transport's numbers. Unset, every leg
        # uses the committed tests/read-cpu-baseline.json.
        leg_baseline="${READ_CPU_BASELINE:-}"
        [ -n "$leg_baseline" ] && leg_baseline="${leg_baseline%.json}-$transport.json"
        CONSTELLATION_FUSE_TRANSPORT="$transport" \
            READ_CPU_BASELINE="${leg_baseline:-tests/read-cpu-baseline.json}" \
            READ_CPU_OUT="$RESULTS_DIR/$transport.read-cpu.jsonl" \
            tests/read-cpu-gate.sh || rc=$?
    fi
done
[ "$rc" -eq 0 ] && echo "TRANSPORT MATRIX PASSED (${TRANSPORTS})"
exit "$rc"
