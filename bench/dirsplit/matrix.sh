#!/bin/bash
# matrix.sh CELL...  where CELL is T<k>:<writers> (the full workload and
# configuration set) or X:<rtt_ms>:<s3_ms> (the crossover sweep: the root
# RTT_MS away from 4 writers sitting together, S3 at S3_MS for every
# node, workloads uniform and own, configurations a and c, no b).
# Runs the cells one after the other (one benchmark job at a time).
set -u
here=$(cd "$(dirname "$0")" && pwd)
. "$here/topologies.sh"
for cell in "$@"; do
    case $cell in
    X:*)
        IFS=: read -r _ rtt s3 <<< "$cell"
        NO_B=1 "$here/run-cell.sh" "X-rtt$rtt-s3$s3-w4" rootfar 4 "$s3" "$s3" 0.5 "$rtt" \
            "${REPS:-3}" uniform,own a,c
        ;;
    *)
        IFS=: read -r topo writers <<< "$cell"
        set -- $(topology "$topo") || continue
        "$here/run-cell.sh" "$topo-w$writers" "$1" "$writers" "$2" "$3" "$4" "$5" "${REPS:-3}"
        ;;
    esac
done
