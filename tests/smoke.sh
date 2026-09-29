#!/usr/bin/env bash
# End-to-end smoke test: create FS, mount, exercise POSIX ops, unmount,
# remount, verify persistence.
#
# The test itself is `harness smoke` (crates/harness/src/smoke.rs, a step for
# step port of the script this used to be); this file only finds the harness
# binary and forwards the arguments.
#
# Usage:
#   tests/smoke.sh                    # local file backend (tmpdir)
#   tests/smoke.sh s3://bucket/prefix # any S3-compatible backend
#                                     # (credentials/endpoint via AWS_* env)
#
# Knobs:
#   CONSTELLATION_BIN          binary under test (default
#                              ${CARGO_TARGET_DIR:-target}/debug/constellation)
#   CONSTELLATION_HARNESS_BIN  harness binary to run (default: the `harness`
#                              next to $CONSTELLATION_BIN, i.e. the same build
#                              (`make smoke`, the suite image), else the
#                              target dir's debug/release build, else
#                              `harness` on PATH, else built here with cargo)
set -euo pipefail

target="${CARGO_TARGET_DIR:-target}"
harness="${CONSTELLATION_HARNESS_BIN:-}"
sibling=""
[ -n "${CONSTELLATION_BIN:-}" ] && sibling="$(dirname "$CONSTELLATION_BIN")/harness"
if [ -z "$harness" ]; then
    if [ -n "$sibling" ] && [ -x "$sibling" ]; then
        harness="$sibling"
    elif [ -x "$target/debug/harness" ]; then
        harness="$target/debug/harness"
    elif [ -x "$target/release/harness" ]; then
        harness="$target/release/harness"
    elif command -v harness >/dev/null 2>&1; then
        harness="$(command -v harness)"
    else
        echo "== building the harness (no harness binary found)"
        (cd "$(dirname "$0")/.." && cargo build -q -p constellation-harness)
        harness="$target/debug/harness"
        case "$harness" in /*) ;; *) harness="$(cd "$(dirname "$0")/.." && pwd)/$harness" ;; esac
    fi
fi

exec "$harness" smoke "$@"
