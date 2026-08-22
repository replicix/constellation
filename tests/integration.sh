#!/usr/bin/env bash
# Integration test against a real S3 API (floci via docker compose).
# Used locally for host-side runs; CI uses the fully containerized
# tests/compose-test.sh instead.
#
#   tests/integration.sh           # start floci, run, leave it running
#   tests/integration.sh --down    # ...and tear floci down afterwards
#
# Requires: docker compose, fusermount3, a built `constellation` binary
# (CONSTELLATION_BIN or $CARGO_TARGET_DIR/debug/constellation).
set -euo pipefail

cd "$(dirname "$0")/.."

TEARDOWN=false
[ "${1:-}" = "--down" ] && TEARDOWN=true

cleanup() {
    if $TEARDOWN; then
        docker compose down -v --remove-orphans || true
    fi
}
trap cleanup EXIT

echo "== starting floci (S3 emulator)"
docker compose up -d --wait floci

# The init hook pre-creates the bucket; nothing else to do.

# Credentials/endpoint for the constellation CLI (object_store reads AWS_*).
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT=http://localhost:4566
export AWS_ALLOW_HTTP=true

# Unique prefix per run so reruns never collide.
PREFIX="run-$(date +%s)-$$"
echo "== running smoke test against s3://constellation-ci/$PREFIX"
bash tests/smoke.sh "s3://constellation-ci/$PREFIX"

echo "INTEGRATION TEST PASSED"
