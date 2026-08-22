#!/usr/bin/env bash
# Fully containerized test run: floci S3 + the FUSE suites, all in
# containers. This is what CI runs; it needs only docker on the host.
#
#   tests/compose-test.sh                  # all suites
#   tests/compose-test.sh smoke stress     # selected suites
#   tests/compose-test.sh --down [suites]  # ...tear down afterwards
set -euo pipefail
cd "$(dirname "$0")/.."

TEARDOWN=false
if [ "${1:-}" = "--down" ]; then
    TEARDOWN=true
    shift
fi
SUITES=("${@:-}")
[ -z "${SUITES[0]:-}" ] && SUITES=(smoke compliance stress)

cleanup() {
    if $TEARDOWN; then
        docker compose --profile test down -v --remove-orphans || true
    fi
}
trap cleanup EXIT

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
