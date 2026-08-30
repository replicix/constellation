#!/usr/bin/env bash
# Compare a repeatable local-floci benchmark with the committed rate floor.
set -euo pipefail
cd "$(dirname "$0")/.."

BASELINE="${PERF_BASELINE:-tests/perf-baseline.json}"
HARNESS="${HARNESS_BIN:-target/release/harness}"
[ -x "$HARNESS" ] || { echo "missing release harness: $HARNESS"; exit 2; }

files=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["config"]["files"])' "$BASELINE")
file_size=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["config"]["file_size"])' "$BASELINE")
fanout=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["config"]["fanout"])' "$BASELINE")
repetitions=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["config"].get("repetitions", 3))' "$BASELINE")
results=$(mktemp -d)
trap 'rm -rf "$results"' EXIT

for run in $(seq 1 "$repetitions"); do
    "$HARNESS" bench --json --files "$files" --file-size "$file_size" --fanout "$fanout" \
        >"$results/$run.json"
done

python3 - "$BASELINE" "$results" <<'PY'
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
