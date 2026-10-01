#!/usr/bin/env bash
# Plan 37 K0 Track A: run `handover_probe` as root, with /dev/fuse.
#
# The probe calls mount(2) itself, so it needs CAP_SYS_ADMIN. On a host
# where nobody has root (this one), a privileged container is the shortest
# path to the same kernel; `--in-container` is what produced
# `results/`'s JSON. Inside a kind node or a VM, run the binary directly.
#
# Two recorded runs, in this order:
#   run.sh [--in-container]          the headline run (results/k0-track-a.json)
#   run.sh --deep [--in-container]   80 clients against 8 fuse workers, so
#                                    the kernel's queue sits above
#                                    congestion_threshold and at
#                                    max_background during every pause
#                                    (results/k0-track-a-deep-queue.json)
set -euo pipefail
repo=$(cd "$(dirname "$0")/../.." && pwd)
results=$repo/bench/fuse-handover-probe/results

if [[ ${1:-} == --deep ]]; then
  shift
  out=${OUT:-$results/k0-track-a-deep-queue.json}
  args=(--handoffs 10 --pauses 500,2000,5000,10000 --writers 64 --readers 16
        --span-mib 8 --json "$out")
  fio=()
else
  out=${OUT:-$results/k0-track-a.json}
  args=(--handoffs 40 --pauses 500,2000,5000,10000 --writers 8 --readers 4
        --span-mib 16 --fio-seconds 60 --json "$out")
  fio=(--fio /usr/bin/fio)
fi

if [[ ${1:-} == --in-container ]]; then
  shift
  mkdir -p "$(dirname "$out")"
  exec docker run --rm --privileged --device /dev/fuse \
    -v "$repo:$repo" -w "$repo" ubuntu:24.04 sh -c \
    "apt-get update -qq >/dev/null 2>&1 && apt-get install -y -qq fio >/dev/null 2>&1; \
     exec $repo/target/release/examples/handover_probe ${args[*]} ${fio[*]} $*"
fi

exec "$repo/target/release/examples/handover_probe" "${args[@]}" "$@"
