#!/usr/bin/env bash
# Delete the kind cluster tests/csi/kind-up.sh created. Idempotent.
set -euo pipefail
kind="${KIND_BIN:-kind}"
name="${KIND_CLUSTER:-constellation-csi}"
"$kind" delete cluster --name "$name"
