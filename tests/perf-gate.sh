#!/usr/bin/env bash
# Compare a repeatable local-floci benchmark with the committed rate floor.
set -euo pipefail
cd "$(dirname "$0")/.."

BASELINE="${PERF_BASELINE:-tests/perf-baseline.json}"
HARNESS="${HARNESS_BIN:-target/release/harness}"

results=$(mktemp -d)
trap 'rm -rf "$results"' EXIT

# Plan 31 C7: the in-process VFS dispatch gate first (no docker, fails
# fast). `vfs_bench` itself exits 1 when an op's dispatch overhead reaches
# the §6.9 limit (1 us) or the observed path allocates more than the bare
# one; the ceilings below also fail a regression of the backends' own
# per-op allocations (C7b took the View's 4 KiB read from 60 to 21).
if [ "${PERF_GATE_SKIP_VFS_BENCH:-0}" != 1 ]; then
    VFS_BENCH_REPORT_ONLY=1 VFS_BENCH_JSON="$results/vfs.json" \
        cargo bench -p constellation-engine --bench vfs_bench \
        || { echo "PERFORMANCE REGRESSION: vfs-bench (plan 31 §6.9 targets)" >&2; exit 1; }
    python3 - "$BASELINE" "$results/vfs.json" <<'PY'
import json, sys
gate = json.load(open(sys.argv[1])).get("vfs_bench")
report = json.load(open(sys.argv[2]))
if not report["ok"]:
    raise SystemExit("PERFORMANCE REGRESSION: vfs-bench reported a target miss")
if gate:
    slack = float(gate.get("alloc_slack", 0.5))
    failed = []
    print(f"{'vfs-bench':24} {'overhead ns':>12} {'allocs/op':>10} {'ceiling':>8}")
    for row in report["rows"]:
        key = f"{row['backend']}/{row['op']}"
        ceiling = gate["max_allocs_per_op"].get(key)
        shown = "-" if ceiling is None else f"{ceiling:.0f}"
        print(f"{key:24} {row['overhead_ns']:12.1f} {row['direct_allocs']:10.3f} {shown:>8}")
        if ceiling is not None and row["direct_allocs"] > ceiling + slack:
            failed.append(key)
    if failed:
        raise SystemExit("PERFORMANCE REGRESSION: per-op allocations above the ceiling: " + ", ".join(failed))
print("VFS-BENCH GATE PASSED")
PY
fi
# The PR workflow runs this stage alone (the harness bench is nightly's).
[ "${PERF_GATE_VFS_BENCH_ONLY:-0}" = 1 ] && exit 0

[ -x "$HARNESS" ] || { echo "missing release harness: $HARNESS"; exit 2; }
files=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["config"]["files"])' "$BASELINE")
file_size=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["config"]["file_size"])' "$BASELINE")
fanout=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["config"]["fanout"])' "$BASELINE")
repetitions=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["config"].get("repetitions", 3))' "$BASELINE")

mkdir -p "$results/bench"
for run in $(seq 1 "$repetitions"); do
    "$HARNESS" bench --json --files "$files" --file-size "$file_size" --fanout "$fanout" \
        >"$results/bench/$run.json"
done

python3 - "$BASELINE" "$results/bench" <<'PY'
import json
from pathlib import Path
from statistics import median
import sys

baseline = json.load(open(sys.argv[1]))
runs = [json.load(path.open()) for path in sorted(Path(sys.argv[2]).glob("*.json"))]
actual = {metric: median(run[metric] for run in runs) for metric in baseline["metrics"]}
tolerance = float(baseline["tolerance"])
failed = []
print(f"{'metric':42} {'baseline':>12} {'actual':>12} {'floor':>12}")
for metric, expected in baseline["metrics"].items():
    measured = float(actual[metric])
    floor = float(expected) * (1.0 - tolerance)
    print(f"{metric:42} {expected:12.2f} {measured:12.2f} {floor:12.2f}")
    if measured < floor:
        failed.append(metric)
if failed:
    print("PERFORMANCE REGRESSION: " + ", ".join(failed), file=sys.stderr)
    raise SystemExit(1)
print("PERFORMANCE GATE PASSED")
PY
