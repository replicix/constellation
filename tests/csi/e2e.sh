#!/usr/bin/env bash
# Plan 37 K7: the Kubernetes external storage e2e suite against
# constellation-csi on kind (§12, §13's kind-e2e job; §18 item 3).
#
#   1. kind cluster (tests/csi/kind-up.sh: two FUSE-capable workers) + the
#      chart + the snapshot CRDs and snapshot-controller;
#   2. a private floci S3 on the `kind` docker network; its credentials as a
#      Secret; the two classes tests/csi/testdriver.yaml names, both
#      `constellation-e2e`: a pool StorageClass (a prefix of its own per
#      setup, so a reused cluster never meets an engine pod of an earlier
#      setup's filesystem; allowVolumeExpansion) and a VolumeSnapshotClass;
#      then the chart's `helm test` on that class (a PVC, a writer pod, a
#      reader pod on a fresh mount);
#   3. the upstream `e2e.test` (matching the kind node image's Kubernetes):
#      -ginkgo.focus='External.Storage' -storage.testdriver=tests/csi/testdriver.yaml,
#      every spec the driver definition selects — in E2E_PROCS parallel
#      processes through the `ginkgo` CLI when it is there ([Serial] specs
#      still run alone, after the rest), else in one e2e.test process. No
#      skip regex: what is skipped is skipped by the suite itself, from the
#      declared capabilities or the cluster, and tests/csi/e2e-skips.md
#      says why for each;
#   4. a tally from the JUnit report (tests/csi/e2e-tally.py): passed /
#      failed / skipped, each skipped spec with the reason the suite gave.
#
# The verdict is e2e.test's: any failed spec fails the script.
#
# Usage: tests/csi/e2e.sh [extra e2e.test flags...]
# Knobs:
#   E2E_TEST_BIN         the e2e.test binary (default: `e2e.test` on PATH)
#   GINKGO_BIN           the ginkgo CLI (default: `ginkgo` on PATH; without
#                        it the specs run serially in e2e.test itself)
#   E2E_PROCS            parallel processes with ginkgo (default 4)
#   E2E_FOCUS            the focus regex (default External.Storage); a
#                        narrower one must still start with
#                        `External.Storage` — it selects a slice of the
#                        suite (a reused cluster's chunked run), never
#                        skips what fails
#   E2E_REUSE=1          use the cluster and classes a previous
#                        `E2E_SETUP_ONLY=1` (or `CSI_KEEP=1`) run left (no
#                        build, no install), and leave them
#   E2E_SETUP_ONLY=1     set up (and keep) the cluster, run nothing
#   KIND_BIN, KIND_CLUSTER (default csi-e2e), KIND_CONFIG_DRAFT,
#   KIND_NODE_IMAGE      as for kind-up.sh
#   CSI_IMAGE            image to load (default constellation-csi:dev; built
#                        with `make csi-image` unless CSI_SKIP_BUILD=1)
#   E2E_REPORT_DIR       where the JUnit report, the log and the summary go
#                        (default target/csi-e2e)
#   E2E_REPORT_NAME      their base name (default e2e: e2e.xml, e2e.log,
#                        e2e.summary.txt)
#   E2E_TIMEOUT          ginkgo's suite timeout (default 6h)
#   CSI_KEEP=1           leave the cluster and the S3 container running
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/csi/lib.sh"
kind="${KIND_BIN:-kind}"
e2e="${E2E_TEST_BIN:-e2e.test}"
ginkgo="${GINKGO_BIN:-ginkgo}"
procs="${E2E_PROCS:-4}"
export KIND_CLUSTER="${KIND_CLUSTER:-csi-e2e}"
export CSI_IMAGE="${CSI_IMAGE:-constellation-csi:dev}"
export CSI_NAMESPACE="${CSI_NAMESPACE:-constellation-csi}"
ctx="kind-$KIND_CLUSTER"
ns="$CSI_NAMESPACE"
s3c="$KIND_CLUSTER-floci"
bucket=csi-e2e
class=constellation-e2e
report="${E2E_REPORT_DIR:-$root/target/csi-e2e}"
name="${E2E_REPORT_NAME:-e2e}"
focus="${E2E_FOCUS:-External.Storage}"
reuse="${E2E_REUSE:-0}"
k() { kubectl --context "$ctx" "$@"; }

command -v "$e2e" >/dev/null || { echo "no e2e.test binary ($e2e); set E2E_TEST_BIN" >&2; exit 1; }
case "$focus" in
External.Storage*) ;;
*) echo "E2E_FOCUS must start with External.Storage (got $focus)" >&2; exit 1 ;;
esac
for a in "$@"; do
    case "$a" in
    -ginkgo.skip* | --ginkgo.skip* | -skip* | --skip*)
        echo "e2e.sh refuses $a: skips are listed in tests/csi/e2e-skips.md, never hidden by a regex" >&2
        exit 1 ;;
    esac
done
if [ "$reuse" = 1 ] || [ "${E2E_SETUP_ONLY:-0}" = 1 ]; then
    export CSI_KEEP=1
fi

kubeconfig=""
cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        echo "== FAILED ($status); driver state"
        k -n "$ns" get pods -o wide 2>/dev/null || true
        k -n "$ns" logs -l app.kubernetes.io/component=controller -c constellation-csi \
            --tail=40 --prefix 2>/dev/null || true
        k -n "$ns" logs -l app.kubernetes.io/component=node -c constellation-csi --tail=40 \
            --prefix 2>/dev/null || true
    fi
    if [ "${CSI_KEEP:-0}" != 1 ]; then
        "$kind" delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
        docker rm -f "$s3c" >/dev/null 2>&1 || true
    fi
    [ -n "$kubeconfig" ] && rm -f -- "$kubeconfig"
    exit "$status"
}
trap cleanup EXIT

setup() {
    build_image "$CSI_IMAGE"
    echo "== kind cluster $KIND_CLUSTER + chart ($CSI_IMAGE)"
    "$root/tests/csi/kind-up.sh" --install

    echo "== S3 (floci) on the kind network"
    local endpoint run_id
    endpoint=$(floci_up "$s3c" "$bucket")
    run_id=$(date +%s)
    echo "   endpoint $endpoint, bucket $bucket, pool prefix csi-e2e/$run_id"

    echo "== classes $class (StorageClass, VolumeSnapshotClass)"
    k -n "$ns" create secret generic constellation-e2e-creds \
        --from-literal=aws_access_key_id=test --from-literal=aws_secret_access_key=test \
        --dry-run=client -o yaml | k apply -f - >/dev/null
    # A class is immutable in its parameters: a reused cluster gets it anew.
    k delete storageclass "$class" --ignore-not-found >/dev/null
    k apply -f - >/dev/null <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: $class
provisioner: constellation.csi.replicix.com
parameters:
  bucket: "$bucket"
  prefix: "csi-e2e/$run_id"
  endpoint: "$endpoint"
  region: "us-east-1"
  layout: "pool"
  chunkSize: "1MiB"
  csi.storage.k8s.io/provisioner-secret-name: constellation-e2e-creds
  csi.storage.k8s.io/provisioner-secret-namespace: "$ns"
  csi.storage.k8s.io/node-stage-secret-name: constellation-e2e-creds
  csi.storage.k8s.io/node-stage-secret-namespace: "$ns"
  csi.storage.k8s.io/controller-expand-secret-name: constellation-e2e-creds
  csi.storage.k8s.io/controller-expand-secret-namespace: "$ns"
reclaimPolicy: Delete
volumeBindingMode: Immediate
allowVolumeExpansion: true
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: $class
driver: constellation.csi.replicix.com
deletionPolicy: Delete
parameters:
  csi.storage.k8s.io/snapshotter-secret-name: constellation-e2e-creds
  csi.storage.k8s.io/snapshotter-secret-namespace: "$ns"
EOF

    # The chart's own post-install check (plan 37 §14, §18 item 4) on the
    # class just made. `--reuse-values` also renders every number the
    # install set through the JSON round trip (tests/csi/chart-check.sh).
    echo "== helm test (tests.storageClassName=$class)"
    helm --kube-context "$ctx" upgrade constellation-csi "$root/deploy/helm/constellation-csi" \
        -n "$ns" --reuse-values --set tests.storageClassName="$class" --wait --timeout 180s >/dev/null
    # A passing test's hooks are deleted (hook-succeeded); a failing one's
    # stay, and their logs say why.
    if ! helm --kube-context "$ctx" test constellation-csi -n "$ns" --timeout 600s; then
        k -n "$ns" logs constellation-csi-test-write --tail=50 2>/dev/null || true
        k -n "$ns" logs constellation-csi-test-read --tail=50 2>/dev/null || true
        k -n "$ns" describe pvc constellation-csi-test-volume 2>/dev/null | tail -20 || true
        exit 1
    fi
}

if [ "$reuse" = 1 ]; then
    k get storageclass "$class" >/dev/null \
        || { echo "E2E_REUSE=1, but cluster $KIND_CLUSTER has no class $class" >&2; exit 1; }
    echo "== reusing cluster $KIND_CLUSTER and its classes"
else
    setup
fi
if [ "${E2E_SETUP_ONLY:-0}" = 1 ]; then
    echo "== set up and kept: E2E_REUSE=1 runs against it; KIND_CLUSTER=$KIND_CLUSTER tests/csi/kind-down.sh and docker rm -f $s3c remove it"
    exit 0
fi

kubeconfig=$(mktemp)
"$kind" get kubeconfig --name "$KIND_CLUSTER" >"$kubeconfig"
mkdir -p "$report"
xml="$report/$name.xml" log="$report/$name.log" summary="$report/$name.summary.txt"
flags=(
    -kubeconfig "$kubeconfig"
    -storage.testdriver="$root/tests/csi/testdriver.yaml"
    "$@"
)
echo "== $("$e2e" --version 2>/dev/null || echo e2e.test): $focus (log: $log)"
: >"$log"
set +e
if command -v "$ginkgo" >/dev/null && [ "$procs" -gt 1 ]; then
    echo "   $procs parallel processes (ginkgo $("$ginkgo" version 2>/dev/null | awk '{print $NF}'))"
    "$ginkgo" --procs="$procs" --focus="$focus" --junit-report="$xml" \
        --timeout="${E2E_TIMEOUT:-6h}" --no-color -v "$e2e" -- "${flags[@]}" >"$log" 2>&1
else
    "$e2e" -ginkgo.focus="$focus" -ginkgo.junit-report="$xml" \
        -ginkgo.timeout="${E2E_TIMEOUT:-6h}" -ginkgo.no-color -ginkgo.v "${flags[@]}" \
        >"$log" 2>&1
fi
verdict=$?
set -e

[ -s "$xml" ] || { echo "e2e.test wrote no JUnit report ($verdict); see $log"; exit 1; }
python3 "$root/tests/csi/e2e-tally.py" "$xml" >"$summary"
echo "== tally ($summary)"
sed -n '1,/^PASSED:/p' "$summary" | sed '$d'
if [ "$verdict" -ne 0 ]; then
    echo "== e2e.test failed ($verdict); see $log"
    exit "$verdict"
fi
echo "== External.Storage e2e PASSED: $(head -1 "$summary")"
