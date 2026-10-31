#!/bin/bash
# Plan 40 section 8: run the cells of a job list one at a time, resumable.
# usage: abba.sh JOBS OUTDIR
#   JOBS lines: "STORE VERSION RUN"   STORE: ext4 xfs btrfs tmpfs   VERSION: base off auto always pair
#   base = bin/base with no knob (the build before plan 40's listing commits); off|auto|always = bin/main with
#   CONSTELLATION_FUSE_READDIRPLUS set. Interleave the versions in the job list (ABBA) so the host's drift cancels.
# Needs $D (default ~/cst-p40-8) with bin/base, bin/main, tree.tar (gen-tree.py), and this directory's cell.py/listtool.py.
# A cell is skipped when OUTDIR/done has its line. Environment for cell.py (SIZES, REPS, PHASES, ...) is passed on.
set -u
JOBS=$1; OUT=$2
D=${D:-$HOME/cst-p40-8}
mkdir -p "$OUT"; touch "$OUT/done"
export TREE=${TREE:-$D/tree.tar}
PHASES=${PHASES:-list,write}
while read -r store ver run; do
  [ -z "$store" ] && continue
  key="$store $ver $run"
  grep -qx "$key" "$OUT/done" && continue
  case $store in
    ext4) dir=/mnt/fs-ext4 ;;
    xfs) dir=/mnt/fs-xfs ;;
    btrfs) dir=/mnt/fs-btrfs ;;
    tmpfs) dir=/dev/shm ;;
    *) echo "bad store $store" >> "$OUT/driver.log"; continue ;;
  esac
  case $ver in
    base) bin=$D/bin/base; mode=none ;;
    off|auto|always) bin=$D/bin/main; mode=$ver ;;
    pair) bin=$D/bin/main; mode=off,auto ;;   # one cell, both modes alternated (ABBA) within one populated state dir
    *) echo "bad version $ver" >> "$OUT/driver.log"; continue ;;
  esac
  f="$OUT/cell-$store-$ver-r$run.jsonl"
  rm -f "$f"
  echo "$(date +%T) start $key (load $(cut -d' ' -f1 /proc/loadavg))" >> "$OUT/driver.log"
  timeout --kill-after=30s 21600 python3 "$(dirname "$(readlink -f "$0")")/cell.py" "$bin" "$dir" "$store-$ver-r$run" "$f" \
    --mode "$mode" --phases "$PHASES" > "$OUT/cell-$store-$ver-r$run.log" 2>&1
  rc=$?
  echo "$(date +%T) end $key rc=$rc" >> "$OUT/driver.log"
  if [ $rc = 0 ] && grep -q '"kind": "cell_end"' "$f"; then echo "$key" >> "$OUT/done"; fi
done < "$JOBS"
echo "ABBA-DONE $(date +%T)" >> "$OUT/driver.log"
