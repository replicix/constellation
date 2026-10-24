#!/bin/bash
# xc-workload.sh WORKDIR SECS LOG TREE SRCREPO
# Plan 40 section 7's mix, in a loop until the deadline, inside WORKDIR (on the xcheckfs mount):
# git clone + status/checkout, rsync -a there and back, find -ls, ls -lR, du -s, tar c | tar x,
# rm -rf and find -delete of 100,000-entry directories, stress-ng --dir --dirmany --getdents --link --rename.
# Every step is bounded; its rc and duration go to LOG. A step that fails is a line in LOG, not the end of the run.
W=$1; SECS=$2; LOG=$3; TREE=$4; SRC=$5
END=$(( $(date +%s) + SECS ))
mkdir -p "$W"
left() { [ "$(date +%s)" -lt "$END" ]; }
step() { # NAME CMD...
  left || return 1
  local n=$1; shift
  local t0; t0=$(date +%s)
  timeout --kill-after=30s 1200 "$@" > /dev/null 2> "$LOG.err.$n"
  local rc=$?
  echo "$(date +%T) $n rc=$rc $(( $(date +%s) - t0 ))s" >> "$LOG"
  [ -s "$LOG.err.$n" ] || rm -f "$LOG.err.$n"
  return 0
}
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
branch=$(git -C "$SRC" rev-parse --abbrev-ref HEAD)
i=0
while left; do
  i=$((i+1))
  step clone$i git clone -q "$SRC" "$W/clone$i"
  for k in 1 2 3; do
    step status$i git -C "$W/clone$i" status --porcelain
    step checkout_alt$i git -C "$W/clone$i" checkout -q alt
    step status_alt$i git -C "$W/clone$i" status --porcelain
    step checkout_back$i git -C "$W/clone$i" checkout -q "$branch"
  done
  step rsync_in$i rsync -a "$TREE/" "$W/rs$i/"
  step rsync_back$i rsync -a "$W/rs$i/" "$W/rb$i/"
  step find_ls$i find "$W/rs$i" -ls
  step ls_lR$i ls -lR "$W/rs$i"
  step du_s$i du -s "$W/rs$i"
  step tarcx$i sh -c "tar -C '$W/rs$i' -cf - d0000 d0001 d0002 d0003 d0004 d0005 d0006 d0007 d0008 d0009 | tar -C '$W/rb$i' -xf - --one-top-level=tx"
  step big_rm$i sh -c "python3 '$HERE/mkbig.py' '$W/big1/d' 100000 && rm -rf '$W/big1'"
  step big_find_delete$i sh -c "python3 '$HERE/mkbig.py' '$W/big2/d' 100000 && find '$W/big2' -delete"
  step stress$i sh -c "mkdir -p '$W/stress$i' && cd '$W/stress$i' && stress-ng --temp-path '$W/stress$i' --dir 2 --dirmany 2 --getdent 2 --link 2 --rename 2 -t 90s --metrics-brief"
  step cleanup$i rm -rf "$W/clone$i" "$W/rs$i" "$W/rb$i" "$W/stress$i"
done
echo "$(date +%T) done after $i iterations" >> "$LOG"
