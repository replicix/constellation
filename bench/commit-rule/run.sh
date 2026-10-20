#!/usr/bin/env bash
# Plan 43 C0: run the `ack-latency` harness scenario (p50/p90/p99 of the
# metadata ops per topology and actor, crates/harness/src/scenarios/commit.rs)
# on one or more file systems and write bench/commit-rule/RESULTS.md.
#
#   bench/commit-rule/run.sh [--fs ext4=/mnt/a,xfs=/mnt/b,btrfs=/mnt/c,tmpfs=/dev/shm]
#                            [--runs N] [--samples N] [--out FILE]
#
# `--fs name=dir,...`: the directory is the scenario's TMPDIR (the daemons'
# state dirs and mounts live under it, so it is the file system measured);
# default `default=$TMPDIR` (or /tmp). `--runs` (default 5): runs per file
# system; p50 and p90 gate, p99 is reported (five runs cannot gate a p99).
# `--samples` (default 300): ops per op kind and actor in one run.
#
# The net-wan topology (`CONSTELLATION_HARNESS_LOAD=net-wan`, a `tc netem`
# delay on loopback) needs `sudo -n tc`; without it that part is skipped with
# a note. Everything runs on the release build.
set -euo pipefail
cd "$(dirname "$0")/../.."

FS_SPEC="default=${TMPDIR:-/tmp}"
RUNS=5
SAMPLES=300
OUT="bench/commit-rule/RESULTS.md"
while [ $# -gt 0 ]; do
    case "$1" in
        --fs) FS_SPEC="$2"; shift 2 ;;
        --runs) RUNS="$2"; shift 2 ;;
        --samples) SAMPLES="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

unset CARGO_TARGET_DIR
cargo build --release -p constellation-cli -p constellation-harness
HARNESS="${HARNESS_BIN:-target/release/harness}"

results=$(mktemp -d)
trap 'rm -rf "$results"' EXIT

wan=1
if ! sudo -n true 2>/dev/null || ! command -v tc >/dev/null 2>&1; then
    echo "note: no 'sudo -n tc': the net-wan topology (5) is skipped" >&2
    wan=0
fi

run_one() { # <fs-name> <dir> <load-label|none> <run-number>
    local name=$1 dir=$2 load=$3 run=$4 out
    out="$results/$name-$load-$run.json"
    local -a env=(TMPDIR="$dir" ACK_LATENCY_SAMPLES="$SAMPLES")
    [ "$load" != none ] && env+=(CONSTELLATION_HARNESS_LOAD="$load")
    env "${env[@]}" "$HARNESS" run ack-latency >"$results/stdout.txt" 2>"$results/$name-$load-$run.log" \
        || { echo "ack-latency failed on $name ($load, run $run); log: $results/$name-$load-$run.log" >&2
             tail -20 "$results/$name-$load-$run.log" >&2; exit 1; }
    # The scenario prints one JSON document; the harness adds its own lines.
    grep '^{.*"scenario":"ack-latency"' "$results/stdout.txt" >"$out" \
        || { echo "no ack-latency JSON from $name ($load, run $run)" >&2; exit 1; }
}

IFS=',' read -ra FSES <<<"$FS_SPEC"
for spec in "${FSES[@]}"; do
    name=${spec%%=*}
    dir=${spec#*=}
    for run in $(seq 1 "$RUNS"); do
        echo "== $name run $run/$RUNS" >&2
        run_one "$name" "$dir" none "$run"
        if [ "$wan" = 1 ]; then
            run_one "$name" "$dir" net-wan "$run"
        fi
    done
done

python3 - "$results" "$OUT" "$RUNS" "$SAMPLES" <<'PY'
import glob, json, os, statistics, sys
results, out, runs, samples = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
# (fs, load) -> (topology, actor, op) -> [(p50, p90, p99)] over runs
data = {}
for path in sorted(glob.glob(os.path.join(results, "*.json"))):
    base = os.path.basename(path)[: -len(".json")]
    fs, load, _run = base.rsplit("-", 2)
    doc = json.load(open(path))
    for r in doc["results"]:
        key = (fs, doc["load"])
        data.setdefault(key, {}).setdefault(
            (r["topology"], r["actor"], r["op"]), []
        ).append((r["p50_us"], r["p90_us"], r["p99_us"]))
lines = [
    "# Commit rule: `ack-latency` results (plan 43)",
    "",
    f"Medians over {runs} runs of p50 and p90, the largest p99 of the runs, in",
    f"microseconds; {samples} samples per op and actor and run (p50 and p90 gate, p99 is",
    "reported: a handful of runs cannot gate a p99). Produced by",
    "`bench/commit-rule/run.sh` from the `ack-latency` scenario",
    "(`crates/harness/src/scenarios/commit.rs`).",
    "",
]
for (fs, load), rows in sorted(data.items()):
    lines += [
        f"## {fs} (load: {load})",
        "",
        "| topology | actor | op | p50 (us) | p90 (us) | max p99 (us) |",
        "|---|---|---|---:|---:|---:|",
    ]
    for (topology, actor, op), v in sorted(rows.items()):
        lines.append(
            f"| {topology} | {actor} | {op} | {int(statistics.median(x[0] for x in v))} "
            f"| {int(statistics.median(x[1] for x in v))} | {max(x[2] for x in v)} |"
        )
    lines.append("")
open(out, "w").write("\n".join(lines))
print(f"wrote {out}")
PY
