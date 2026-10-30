#!/bin/bash
# Plan 45 section 7 (box): run the cells of a job list one at a time, resumable.
# usage: abba.sh JOBS OUTDIR
#   JOBS lines: "STORE VERSION RUN"   STORE: ext4 xfs btrfs tmpfs   VERSION: h0 head headnp
#   h0 = $D/bin/cst-h0 (the build of the baseline commit, 1bc76d4), head = $D/bin/cst-head (the build under test),
#   headnp = the build under test with CONSTELLATION_FUSE_READDIRPLUS=off (the baseline predates plan 40's plus
#   entries, so this is the like-for-like for the listing cells).
#   Interleave the versions in the job list (A B B A, then B A A B) so the host's drift cancels.
# Needs $D (default ~/cst-p45) with bin/cst-h0 and bin/cst-head, and this directory's cell.py.
# BURST_ROUNDS_HEAD (default 3): link+unlink cycles of the burst phase for the build under test (more samples for the
# p99; the baseline's cycles take minutes, so it does one). A cell is skipped when OUTDIR/done has its line. PHASES (default link,burst,control,list,trees) and the cell's own
# environment (REPS, BIG_COLD, BIG_WARM, LOAD_MAX, ...) are passed on.
set -u
JOBS=$1; OUT=$2
D=${D:-$HOME/cst-p45}
mkdir -p "$OUT"; touch "$OUT/done"
PHASES=${PHASES:-link,burst,control,list,trees}
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
    h0) bin=$D/bin/cst-h0 ;;
    head) bin=$D/bin/cst-head ;;
    headnp) bin=$D/bin/cst-head ;;
    *) echo "bad version $ver" >> "$OUT/driver.log"; continue ;;
  esac
  f="$OUT/cell-$store-$ver-r$run.jsonl"
  rm -f "$f"
  echo "$(date +%T) start $key (load $(cut -d' ' -f1 /proc/loadavg))" >> "$OUT/driver.log"
  rounds=1; [ "$ver" != h0 ] && rounds=${BURST_ROUNDS_HEAD:-3}
  rdp=auto; [ "$ver" = headnp ] && rdp=off
  CONSTELLATION_FUSE_READDIRPLUS=$rdp BURST_ROUNDS=$rounds timeout --kill-after=30s ${CELL_TIMEOUT:-2700} python3 "$(dirname "$(readlink -f "$0")")/cell.py" "$bin" "$dir" "$store-$ver-r$run" "$f" \
    --phases "$PHASES" > "$OUT/cell-$store-$ver-r$run.log" 2>&1
  rc=$?
  echo "$(date +%T) end $key rc=$rc" >> "$OUT/driver.log"
  if [ $rc = 0 ] && grep -q '"kind": "cell_end"' "$f"; then echo "$key" >> "$OUT/done"; fi
done < "$JOBS"
echo "ABBA-DONE $(date +%T)" >> "$OUT/driver.log"
