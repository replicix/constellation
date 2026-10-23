#!/usr/bin/env bash
# Plan 41 L8: the lock renewal lane's overhead and stall tolerance.
#
#   bench/lock-lane/run.sh [--fs ext4=/mnt/a,xfs=/mnt/b,btrfs=/mnt/c,tmpfs=/dev/shm]
#                          [--runs N] [--stalls MS,MS,...] [--out FILE]
#                          [--skip latency|stall]
#
# For each file system (the directory is the scenarios' TMPDIR, so it is the
# file system the daemons' state dirs and mounts live on) and each of
# `CONSTELLATION_LOCK_LANE=on` and `off`, alternated run by run (base, then
# after, so host drift hits both):
#
# - `lock-latency`: first lock from a non-sequencer, cached re-lock, the
#   sequencer's own lock, a contended handoff, a lone node's flock / fcntl /
#   write under a lock (the lane's cost is in none of them: the acceptance
#   is "no regression", at most 3 % or one IQR);
# - `lock-stall-lone-node` with the lane on and `CONSTELLATION_HARNESS_LONE_LANE=off`
#   (stall tolerance: the lane keeps the lock; the lane-off arm must lapse).
#
# Everything runs on the release build. Writes bench/lock-lane/RESULTS.md.
set -euo pipefail
cd "$(dirname "$0")/../.."

FS_SPEC="default=${TMPDIR:-/tmp}"
RUNS=5
STALLS="5000,12000,20000"
OUT="bench/lock-lane/RESULTS.md"
SKIP=""
while [ $# -gt 0 ]; do
    case "$1" in
        --fs) FS_SPEC="$2"; shift 2 ;;
        --runs) RUNS="$2"; shift 2 ;;
        --stalls) STALLS="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        --skip) SKIP="$2"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

if [ -z "${HARNESS_BIN:-}" ]; then
    unset CARGO_TARGET_DIR
    cargo build --release -p constellation -p constellation-harness
fi
HARNESS="${HARNESS_BIN:-target/release/harness}"

results=$(mktemp -d)
trap 'rm -rf "$results"' EXIT

run_latency() { # <fs-name> <dir> <lane> <run>
    local name=$1 dir=$2 lane=$3 run=$4
    TMPDIR="$dir" CONSTELLATION_LOCK_LANE="$lane" \
        CONSTELLATION_HARNESS_DOCKER_PREFIX="${CONSTELLATION_HARNESS_DOCKER_PREFIX:-cst-lock-lane}" \
        timeout --kill-after=30s 900 "$HARNESS" run lock-latency \
        >"$results/$name-$lane-$run.out" 2>"$results/$name-$lane-$run.log" \
        || { echo "lock-latency failed on $name (lane $lane, run $run); log: $results/$name-$lane-$run.log" >&2
             tail -20 "$results/$name-$lane-$run.log" >&2; exit 1; }
}

run_stall() { # <fs-name> <dir> <lane-env> <run>
    local name=$1 dir=$2 lane=$3 run=$4
    local -a env=(TMPDIR="$dir" CONSTELLATION_HARNESS_LONE_STALLS_MS="$STALLS"
        CONSTELLATION_HARNESS_DOCKER_PREFIX="${CONSTELLATION_HARNESS_DOCKER_PREFIX:-cst-lock-lane}")
    [ "$lane" = off ] && env+=(CONSTELLATION_HARNESS_LONE_LANE=off)
    local rc=0
    env "${env[@]}" timeout --kill-after=30s 1500 "$HARNESS" run lock-stall-lone-node \
        >"$results/$name-stall-$lane-$run.out" 2>"$results/$name-stall-$lane-$run.log" || rc=$?
    echo "$rc" >"$results/$name-stall-$lane-$run.rc"
}

IFS=',' read -ra FSES <<<"$FS_SPEC"
for spec in "${FSES[@]}"; do
    name=${spec%%=*}
    dir=${spec#*=}
    for run in $(seq 1 "$RUNS"); do
        echo "== $name run $run/$RUNS" >&2
        if [ "$SKIP" != latency ]; then
            # Alternated: off (the base), on (the after).
            run_latency "$name" "$dir" off "$run"
            run_latency "$name" "$dir" on "$run"
        fi
        if [ "$SKIP" != stall ]; then
            run_stall "$name" "$dir" on "$run"
            run_stall "$name" "$dir" off "$run"
        fi
    done
done

python3 - "$results" "$OUT" "$RUNS" "$STALLS" <<'PY'
import glob, os, re, statistics, sys
results, out, runs, stalls = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

def secs(tok):  # "12.5ms" / "1.2s" / "340µs" / "7ns" -> microseconds
    m = re.fullmatch(r"([0-9.]+)(ns|µs|us|ms|s)", tok)
    v, u = float(m.group(1)), m.group(2)
    return v * {"ns": 1e-3, "µs": 1, "us": 1, "ms": 1e3, "s": 1e6}[u]

# (fs, lane) -> label -> [(p50, p90)] over runs
lat = {}
pat = re.compile(r"lock-latency: (.*?): n=\d+ p50=(\S+) p90=(\S+) p99=(\S+) max=(\S+)")
for path in sorted(glob.glob(os.path.join(results, "*.log"))):
    base = os.path.basename(path)[: -len(".log")]
    if "-stall-" in base:
        continue
    fs, lane, _run = base.rsplit("-", 2)
    for line in open(path, errors="replace"):
        for m in re.finditer(r"lock-latency: ([^:]+(?:: [^:n]+)?): n=\d+ p50=(\S+) p90=(\S+)", line):
            lat.setdefault((fs, lane), {}).setdefault(m.group(1).strip(), []).append(
                (secs(m.group(2)), secs(m.group(3))))
lines = [
    "# Lock renewal lane: results (plan 41 L8)",
    "",
    f"`bench/lock-lane/run.sh`: {runs} runs per file system and lane setting, alternated",
    "(lane off, then on), release build. `lock-latency` medians of p50 and p90 over runs,",
    "microseconds; the acceptance is no regression (at most 3 % or one IQR of the runs).",
    "",
]
for (fs, lane), rows in sorted(lat.items()):
    lines += [f"## {fs}, lane {lane}", "", "| measurement | p50 (us) | p90 (us) |", "|---|---:|---:|"]
    for label, v in sorted(rows.items()):
        lines.append(f"| {label} | {statistics.median(x[0] for x in v):.0f} | {statistics.median(x[1] for x in v):.0f} |")
    lines.append("")

# Stall tolerance: per fs and lane, how many runs passed (the lane-on arm must pass,
# the lane-off arm must fail its no-lapse check by lapsing: it passes iff the lapse showed).
lines += [f"## Stall tolerance (`lock-stall-lone-node`, stalls {stalls} ms)", "",
          "| file system | lane | runs passed | runs failed |", "|---|---|---:|---:|"]
tally = {}
for path in sorted(glob.glob(os.path.join(results, "*-stall-*.rc"))):
    base = os.path.basename(path)[: -len(".rc")]
    fs, rest = base.split("-stall-")
    lane = rest.split("-")[0]
    ok = open(path).read().strip() == "0"
    t = tally.setdefault((fs, lane), [0, 0])
    t[0 if ok else 1] += 1
for (fs, lane), (p, f) in sorted(tally.items()):
    lines.append(f"| {fs} | {lane} | {p} | {f} |")
lines.append("")
open(out, "w").write("\n".join(lines))
print(f"wrote {out}")
PY
