#!/usr/bin/env bash
# Fully containerized test run: floci S3 + the FUSE suites, all in
# containers. This is what CI runs; it needs only docker on the host.
#
#   tests/compose-test.sh                  # all suites
#   tests/compose-test.sh smoke stress     # selected suites
#   tests/compose-test.sh --keep [suites]  # leave the compose project up afterwards
#
# The compose project is torn down in an EXIT trap however the script ends
# (failure, SIGTERM, a `timeout`), but only if this run started its floci:
# one already up (a shared checkout's) is someone else's.
set -euo pipefail
cd "$(dirname "$0")/.."

TEARDOWN=true
if [ "${1:-}" = "--keep" ]; then
    TEARDOWN=false
    shift
fi
if [ -n "$(docker compose --profile test ps -q floci 2>/dev/null)" ]; then
    echo "== floci is already up: reusing it, and leaving the project up"
    TEARDOWN=false
fi
SUITES=("${@:-}")
[ -z "${SUITES[0]:-}" ] && SUITES=(smoke compliance stress)

cleanup() {
    if $TEARDOWN; then
        docker compose --profile test down -v --remove-orphans || true
    fi
}
trap cleanup EXIT
trap 'exit 143' TERM
trap 'exit 130' INT HUP

# CI pre-builds the image (with a shared layer cache) and sets SMOKE_IMAGE;
# locally we build here.
if [ -z "${SMOKE_IMAGE:-}" ]; then
    docker compose --profile test build smoke
fi

for suite in "${SUITES[@]}"; do
    echo "==== suite: $suite ===="
    docker compose --profile test run --rm "$suite"
done
echo "COMPOSE TEST PASSED (${SUITES[*]})"
