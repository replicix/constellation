#!/usr/bin/env bash
# Plan 43 G1 (§10): topology 3's delegate (root on the LAN, no backup)
# against topology 2's holder, two daemon builds measured in alternating
# order (ABBA: A B, B A, A B, ...) so a drift of the host hits both alike.
#
#   bench/commit-rule/g1-abba.sh --a NAME=DIR --b NAME=DIR [--harness PATH]
#       [--rounds N] [--samples N] [--topologies 2,3] [--max-load L]
#       [--out DIR]
#
# `DIR` holds a release `constellation` (and `harness`); `--harness`
# (default: B's) runs every run, with `CONSTELLATION_BIN` naming the
# variant's daemon. `--rounds` (default 8) runs per variant, `--samples`
# (default 200) ops per op kind and actor and run. A run starts once the
# 1-minute load is below `--max-load` (default 4; after 10 minutes it starts
# anyway, and the load is recorded). Each run is bounded by a 30-minute
# timeout. The state dirs live under `$TMPDIR` (the file system measured).
#
# Output in `--out` (default a new temp dir): one JSON per run, `runs.txt`
# (order, load before and after, exit code) and `summary.txt`: per variant
# the medians over the runs of p50 and p90, and G1 (the delegate's p50 /
# p90 against the holder's + 500 us).
set -euo pipefail

A=""; B=""; HARNESS=""; ROUNDS=8; SAMPLES=200; TOPOLOGIES="2,3"; MAX_LOAD=4; OUT=""
while [ $# -gt 0 ]; do
    case "$1" in
        --a) A="$2"; shift 2 ;;
        --b) B="$2"; shift 2 ;;
        --harness) HARNESS="$2"; shift 2 ;;
        --rounds) ROUNDS="$2"; shift 2 ;;
        --samples) SAMPLES="$2"; shift 2 ;;
        --topologies) TOPOLOGIES="$2"; shift 2 ;;
        --max-load) MAX_LOAD="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done
[ -n "$A" ] && [ -n "$B" ] || { echo "--a NAME=DIR and --b NAME=DIR are required" >&2; exit 2; }
NA=${A%%=*}; DA=${A#*=}; NB=${B%%=*}; DB=${B#*=}
HARNESS=${HARNESS:-$DB/harness}
OUT=${OUT:-$(mktemp -d)}
mkdir -p "$OUT"
for d in "$DA" "$DB"; do
    [ -x "$d/constellation" ] || { echo "no constellation binary in $d" >&2; exit 2; }
done
# A docker prefix of its own: the harness clears leftovers under its prefix
# at startup, which would take down another run's S3.
export CONSTELLATION_HARNESS_DOCKER_PREFIX="g1-abba-$$"

wait_quiet() {
    local waited=0
    while awk -v max="$MAX_LOAD" '{ exit !($1 >= max) }' /proc/loadavg && [ $waited -lt 600 ]; do
        sleep 10; waited=$((waited + 10))
    done
    echo $waited
}

run_one() { # <name> <dir> <round>
    local name=$1 dir=$2 round=$3 waited before rc
    waited=$(wait_quiet)
    before=$(cut -d' ' -f1 /proc/loadavg)
    rc=0
    env CONSTELLATION_BIN="$dir/constellation" ACK_LATENCY_SAMPLES="$SAMPLES" \
        ACK_LATENCY_TOPOLOGIES="$TOPOLOGIES" \
        timeout --kill-after=30s 1800 "$HARNESS" run ack-latency \
        >"$OUT/stdout-$name-$round.txt" 2>"$OUT/log-$name-$round.txt" || rc=$?
    grep '^{.*"scenario":"ack-latency"' "$OUT/stdout-$name-$round.txt" >"$OUT/$name-$round.json" || true
    echo "$name round=$round waited=${waited}s load_before=$before load_after=$(cut -d' ' -f1 /proc/loadavg) rc=$rc $(date -u +%T)" >>"$OUT/runs.txt"
}

for r in $(seq 1 "$ROUNDS"); do
    if [ $((r % 2)) = 1 ]; then
        run_one "$NA" "$DA" "$r"; run_one "$NB" "$DB" "$r"
    else
        run_one "$NB" "$DB" "$r"; run_one "$NA" "$DA" "$r"
    fi
done

python3 - "$OUT" "$NA" "$NB" <<'PY' | tee "$OUT/summary.txt"
import glob, json, os, statistics as st, sys
out, names = sys.argv[1], sys.argv[2:]
d = {}
for path in glob.glob(os.path.join(out, "*.json")):
    name = os.path.basename(path)[: -len(".json")].rsplit("-", 1)[0]
    text = open(path).read().strip()
    if not text:
        continue
    for r in json.loads(text)["results"]:
        d.setdefault((name, r["topology"], r["actor"], r["op"]), []).append((r["p50_us"], r["p90_us"]))
def med(name, topo, actor, op):
    v = d.get((name, topo, actor, op), [])
    if not v:
        return None
    return int(st.median(x[0] for x in v)), int(st.median(x[1] for x in v)), len(v)
print("medians over runs of p50 / p90 (us)")
for op in ["create", "write", "rename", "unlink", "chmod"]:
    for name in names:
        h, g = med(name, "2-backup", "holder", op), med(name, "3-delegated", "delegate", op)
        if not h or not g:
            continue
        verdict = " ".join(
            f"{lbl} {'met' if g[i] <= h[i] + 500 else 'over by %d' % (g[i] - h[i] - 500)}"
            for i, lbl in ((0, "p50"), (1, "p90"))
        )
        print(f"{op:7s} {name:10s} holder(2) {h[0]:6d} / {h[1]:6d}  delegate(3) {g[0]:6d} / {g[1]:6d}"
              f"  (n={g[2]})  G1: {verdict}")
PY
echo "results in $OUT" >&2
