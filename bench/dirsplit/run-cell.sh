#!/bin/bash
# One cell of the directory-split matrix: REPS repetitions of a fresh
# cluster mounted with SPLIT=0 running configurations a, c, d (their
# order rotated per repetition), rename on a fresh cluster per
# configuration, and a fresh cluster mounted with SPLIT=20 (set explicitly: the default is off) running b
# (rename last), for every workload. One JSON line per phase goes
# to $OUT/<label>.jsonl, the harness logs next to it.
#
#   run-cell.sh LABEL LAYOUT WRITERS S3_NEAR S3_FAR RTT_LOCAL RTT_REMOTE \
#               [REPS] [WORKLOADS] [CONFIGS]
#
# CONFIGS is the SPLIT=0 cluster's set (default a,c,d); b runs unless
# NO_B=1. Paths: BIN_DIR (constellation, harness), S3_BIN_DIR
# (versitygw, toxiproxy-server), OUT. Every harness run is bounded by
# `timeout`; before the cell the script waits (at most 10 minutes) for
# the 1-minute load average to drop below MAX_LOAD (4).
set -u
label=$1 layout=$2 writers=$3 s3n=$4 s3f=$5 rttl=$6 rttr=$7
reps=${8:-3}
workloads=${9:-uniform,mix,own,rename}
configs=${10:-a,c,d}
: "${BIN_DIR:?}" "${S3_BIN_DIR:?}" "${OUT:?}"
MAX_LOAD=${MAX_LOAD:-4}
mkdir -p "$OUT"
export TMPDIR=${TMPDIR:-/tmp}
export CONSTELLATION_BIN=$BIN_DIR/constellation
export CONSTELLATION_VERSITYGW_BIN=$S3_BIN_DIR/versitygw
export CONSTELLATION_TOXIPROXY_BIN=$S3_BIN_DIR/toxiproxy-server
export DIRSPLIT_LABEL=$label DIRSPLIT_LAYOUT=$layout DIRSPLIT_WRITERS=$writers
export DIRSPLIT_S3_NEAR_MS=$s3n DIRSPLIT_S3_FAR_MS=$s3f
export DIRSPLIT_RTT_LOCAL_MS=$rttl DIRSPLIT_RTT_REMOTE_MS=$rttr
export DIRSPLIT_WORKLOADS=$workloads
export DIRSPLIT_OUT=$OUT/$label.jsonl
export DIRSPLIT_WARM_S=${DIRSPLIT_WARM_S:-5} DIRSPLIT_MEASURE_S=${DIRSPLIT_MEASURE_S:-15}
export DIRSPLIT_BWARM_S=${DIRSPLIT_BWARM_S:-20} DIRSPLIT_THREADS=${DIRSPLIT_THREADS:-4}

wait_load() {
    local i
    for i in $(seq 1 40); do
        awk -v m="$MAX_LOAD" '{exit !($1 < m)}' /proc/loadavg && return 0
        sleep 15
    done
    echo "load still $(cut -d' ' -f1 /proc/loadavg) after 10 minutes; running anyway" >&2
}

one() { # one TAG CONFIGS ROTATE
    local tag=$1 log
    log=$OUT/$label-$tag.log
    sleep 5
    echo "$(date -u +%FT%TZ) $label $tag start (load $(cut -d' ' -f1 /proc/loadavg))" >> "$OUT/progress.txt"
    DIRSPLIT_CONFIGS=$2 DIRSPLIT_ROTATE=$3 timeout --kill-after=30s "${CELL_TIMEOUT:-2400}" \
        "$BIN_DIR/harness" run dirsplit-bench --s3-backend process > "$log" 2>&1
    local rc=$?
    echo "$(date -u +%FT%TZ) $label $tag rc=$rc" >> "$OUT/progress.txt"
    # A run that died leaves its lo qdisc behind: the next one refuses it.
    if [ $rc -ne 0 ] && sudo -n tc qdisc show dev lo | grep -q prio; then
        sudo -n tc qdisc del dev lo root
    fi
}

# rename runs on clusters of its own, one per configuration: a hand
# split's cross-range renames recall and re-delegate the ranges hundreds
# of times, and the root stays slower after that (see RESULTS.md), which
# would skew every later phase on the same cluster.
plain=$(echo ",$workloads," | sed 's/,rename,/,/g; s/^,//; s/,$//')
IFS=, read -r -a cfgs <<< "$configs"
# The host is quiet before the cell. (Between its own runs the load
# average is the decaying echo of the previous run's FUSE waits, which
# count as uninterruptible sleep, not of anything still running.)
wait_load
for rep in $(seq 1 "$reps"); do
    export DIRSPLIT_REP=$rep
    rot=$(( (rep - 1) % ${#cfgs[@]} ))
    order=("${cfgs[@]:$rot}" "${cfgs[@]:0:$rot}")
    if [ -n "$plain" ]; then
        DIRSPLIT_WORKLOADS=$plain one "r$rep-$(IFS=; echo "${order[*]}")" "$configs" "$rot"
    fi
    if [[ ",$workloads," == *,rename,* ]]; then
        for c in "${order[@]}"; do
            DIRSPLIT_WORKLOADS=rename one "r$rep-rename-$c" "$c" 0
        done
    fi
    [ "${NO_B:-0}" = 1 ] || one "r$rep-b" b 0
done
