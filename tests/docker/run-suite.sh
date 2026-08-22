#!/usr/bin/env bash
# Container entrypoint: run one test suite against a backend.
#
#   run-suite.sh [smoke|compliance|stress] [backend-url]
#
# Defaults: smoke suite, unique prefix on the CI bucket.
set -euo pipefail
SUITE="${1:-smoke}"
BACKEND="${2:-${SMOKE_BACKEND:-s3://constellation-ci/${SUITE}-$(date +%s)-$$}}"
case "$SUITE" in
    smoke|compliance|stress) ;;
    *) echo "unknown suite: $SUITE (want smoke|compliance|stress)"; exit 2 ;;
esac
exec bash "${SUITE_DIR:-/usr/local/lib/constellation}/$SUITE.sh" "$BACKEND"
