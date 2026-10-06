#!/usr/bin/env bash
# Plan 37 K7: the constellation-csi chart, checked without a cluster.
#
#   1. `helm lint` (and `--strict`) with the defaults and with a values file
#      that turns every optional template on;
#   2. numbers render as integers. A value reaches the templates as int64
#      from `--set` and as float64 from a values file, `--set-json` and
#      every `helm upgrade --reuse-values` (the release's values round-trip
#      through JSON); `quote`/`toString` print a float64 past six digits in
#      exponent form (`purge.bytesPerSecond` 16777216 -> "1.6777216e+07"),
#      which the controller refused to start on (found in K7a). Every number
#      is rendered through the chart's `csi.str`, and this renders large ones
#      all three ways and greps the output for an exponent and for the exact
#      integers;
#   3. a fractional number is refused at render time, not truncated;
#   4. the `helm test` hook renders with a class (PVC + writer + reader) and
#      without one (a pod that fails saying what to set).
#
# Needs helm. Usage: [CHART=dir] tests/csi/chart-check.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
chart="${CHART:-$root/deploy/helm/constellation-csi}"
tmp=$(mktemp -d)
trap 'rm -rf -- "$tmp"' EXIT
fail=0
pass() { echo "[PASS] $*"; }
bad() { echo "[FAIL] $*"; fail=1; }

cat >"$tmp/all.yaml" <<'EOF'
node: {metricsPort: 9100}
storageClasses:
  - name: pool
    parameters: {bucket: b, prefix: p, layout: pool, shards: 4}
volumeSnapshotClasses:
  - name: snaps
    parameters: {csi.storage.k8s.io/snapshotter-secret-name: s}
credentials:
  watchedSecrets:
    - {namespace: constellation-csi, name: s}
EOF

for args in "" "-f $tmp/all.yaml"; do
    # shellcheck disable=SC2086
    if helm lint --strict "$chart" $args >"$tmp/lint" 2>&1; then
        pass "helm lint --strict ${args:-(defaults)}"
    else
        cat "$tmp/lint"
        bad "helm lint --strict ${args:-(defaults)}"
    fi
done

# name=value pairs: every number the chart renders, set large.
big=(
    controller.replicas=3
    controller.livenessPort=19808
    node.livenessPort=19809
    node.metricsPort=19100
    node.maxUnavailable=10
    node.drain.preStopTimeoutSeconds=12345678
    engineProfile.readyTimeout=10000000
    engineProfile.idleGcInterval=30000000
    engineProfile.handoff.drainTimeoutMs=50000000
    engineProfile.handoff.totalTimeoutMs=300000000
    engineProfile.handoff.resumeTimeoutMs=3000000000
    engineProfile.handoff.maxAttempts=7
    engineProfile.handoff.rolloutInterval=86400000
    purge.maxConcurrentDeletes=12345678
    purge.opsPerSecond=50000000
    purge.bytesPerSecond=16777216
)
set_args=() json=() yaml=""
for kv in "${big[@]}"; do
    set_args+=(--set "$kv")
    k="${kv%%=*}" v="${kv#*=}"
    # --set-json path=value: a float64, as --reuse-values gives.
    json+=(--set-json "$k=$v")
done
# The same as a values file (float64 too): nested YAML from the dotted keys.
yaml=$(python3 - "${big[@]}" <<'EOF'
import json, sys
doc = {}
for kv in sys.argv[1:]:
    k, v = kv.split("=", 1)
    d = doc
    *path, last = k.split(".")
    for p in path:
        d = d.setdefault(p, {})
    d[last] = int(v)
print(json.dumps(doc))
EOF
)
echo "$yaml" >"$tmp/big.json"
shards=(--set 'storageClasses[0].name=pool' --set 'storageClasses[0].parameters.bucket=b'
    --set 'storageClasses[0].parameters.shards=4'
    --set 'storageClasses[0].parameters.quotaBytes=1099511627776')

for how in set set-json values; do
    case $how in
    set) extra=("${set_args[@]}") ;;
    set-json) extra=("${json[@]}") ;;
    values) extra=(-f "$tmp/big.json") ;;
    esac
    out="$tmp/render-$how.yaml"
    if ! helm template t "$chart" "${extra[@]}" "${shards[@]}" >"$out" 2>"$tmp/err"; then
        cat "$tmp/err"
        bad "render with large numbers (--$how)"
        continue
    fi
    if grep -nE '[0-9]e[+-]?[0-9]' "$out"; then
        bad "--$how: a number rendered in exponent form (above)"
    else
        pass "--$how: no number in exponent form"
    fi
    for want in \
        'CONSTELLATION_CSI_PURGE_BYTES_PER_SECOND, value: "16777216"' \
        'CONSTELLATION_CSI_PURGE_OPS_PER_SECOND, value: "50000000"' \
        'CONSTELLATION_CSI_PURGE_MAX_CONCURRENT_DELETES, value: "12345678"' \
        'CONSTELLATION_CSI_HANDOFF_RESUME_TIMEOUT_MS, value: "3000000000"' \
        'CONSTELLATION_CSI_ENGINE_READY_TIMEOUT_S, value: "10000000"' \
        'CONSTELLATION_CSI_IDLE_GC_INTERVAL_S, value: "30000000"' \
        'CONSTELLATION_CSI_ROLLOUT_INTERVAL_S, value: "86400000"' \
        'CONSTELLATION_CSI_PRESTOP_TIMEOUT_S, value: "12345678"' \
        'CONSTELLATION_CSI_METRICS_ADDR, value: "0.0.0.0:19100"' \
        'terminationGracePeriodSeconds: 12345708' \
        'replicas: 3' \
        'rollingUpdate: {maxUnavailable: 10}' \
        '--health-port=19808' \
        'containerPort: 19809' \
        '"shards": "4"' \
        '"quotaBytes": "1099511627776"'; do
        grep -qF -- "$want" "$out" || bad "--$how: missing \`$want\`"
    done
done
# A percentage stays a string.
helm template t "$chart" --set node.maxUnavailable=25% \
    | grep -qF 'rollingUpdate: {maxUnavailable: 25%}' \
    && pass "maxUnavailable 25% renders as given" || bad "maxUnavailable 25%"

for key in purge.opsPerSecond node.drain.preStopTimeoutSeconds; do
    if helm template t "$chart" --set-json "$key=1.5" >/dev/null 2>"$tmp/err"; then
        bad "a fractional $key rendered"
    elif grep -q "expected a whole number, got 1.5" "$tmp/err"; then
        pass "a fractional $key is refused at render time"
    else
        cat "$tmp/err"
        bad "a fractional $key failed for another reason"
    fi
done

tests="$tmp/tests.yaml"
helm template t "$chart" -f "$tmp/all.yaml" --show-only templates/tests/volume-roundtrip.yaml >"$tests"
if [ "$(grep -c '"helm.sh/hook": test' "$tests")" = 3 ] \
    && grep -q 'storageClassName: pool' "$tests" \
    && grep -q 'claimName: t-test-volume' "$tests"; then
    pass "helm test hook: PVC + writer + reader on the first class"
else
    bad "helm test hook with a class"
fi
helm template t "$chart" --show-only templates/tests/volume-roundtrip.yaml >"$tests"
if [ "$(grep -c '"helm.sh/hook": test' "$tests")" = 1 ] && grep -q 'no StorageClass to test' "$tests"; then
    pass "helm test hook without a class fails, saying what to set"
else
    bad "helm test hook without a class"
fi

[ "$fail" = 0 ] && echo "chart-check: all passed" || { echo "chart-check: FAILED"; exit 1; }
