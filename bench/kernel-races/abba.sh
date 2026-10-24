#!/bin/bash
# Plan 42 section 8: run the cells of a job list, one at a time, resumable.
# usage: abba.sh JOBS OUTDIR     JOBS lines: "STORE VERSION RUN" (STORE: ext4 xfs btrfs btrfs-zstd tmpfs; VERSION: base final)
# Needs ~/cst-p42-8/{bin/cst-base,bin/cst-final,kb,tree.tar}. A cell is skipped when OUTDIR/done has its line.
set -u
JOBS=$1; OUT=$2
D=$HOME/cst-p42-8
mkdir -p "$OUT"; touch "$OUT/done"
export KB=$D/kb TREE=$D/tree.tar HERE=$D
btrfs_mode=none
set_btrfs() { # none|zstd
  [ "$btrfs_mode" = "$1" ] && return
  if [ "$1" = zstd ]; then sudo -n mount -o remount,compress=zstd /mnt/fs-btrfs; else sudo -n mount -o remount,compress=no /mnt/fs-btrfs; fi
  btrfs_mode=$1
  echo "btrfs options now: $(grep ' /mnt/fs-btrfs ' /proc/mounts | cut -d' ' -f4)" >> "$OUT/driver.log"
}
trap 'set_btrfs none' EXIT
while read -r store ver run; do
  [ -z "$store" ] && continue
  key="$store $ver $run"
  grep -qx "$key" "$OUT/done" && continue
  case $store in
    ext4) dir=/mnt/fs-ext4 ;;
    xfs) dir=/mnt/fs-xfs ;;
    btrfs) dir=/mnt/fs-btrfs; set_btrfs none ;;
    btrfs-zstd) dir=/mnt/fs-btrfs; set_btrfs zstd ;;
    tmpfs) dir=/dev/shm ;;
    *) echo "bad store $store" >> "$OUT/driver.log"; continue ;;
  esac
  f="$OUT/cell-$store-$ver-r$run.txt"
  rm -f "$f"
  echo "$(date +%T) start $key (load $(cut -d' ' -f1 /proc/loadavg))" >> "$OUT/driver.log"
  FRESH=$([ "$store" = tmpfs ] && echo 1 || echo 0) CACHE=$([ "$store" = tmpfs ] && echo 2G || echo 4G) \
  FINAL=$([ "$ver" = final ] && echo 1 || echo 0) \
    timeout --kill-after=30s 14400 "$D/cell.sh" "$D/bin/cst-$ver" "$dir" "$store-$ver-r$run" "$f" > "$OUT/cell-$store-$ver-r$run.log" 2>&1
  rc=$?
  echo "$(date +%T) end $key rc=$rc" >> "$OUT/driver.log"
  if [ $rc = 0 ] && grep -q '^CELL-END' "$f"; then echo "$key" >> "$OUT/done"; fi
done < "$JOBS"
echo "ABBA-DONE $(date +%T)" >> "$OUT/driver.log"
