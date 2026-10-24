#!/bin/bash
# Plan 42 section 8, attribution runs: one benchmark on several builds, alternating, on one backing store.
# usage: micro.sh STOREDIR OUT TEST ROUNDS label=bin[,ENV=VAL...] ...
#   TEST: commit (5,000 small files written and closed, one view), untar (the 50k-file tree), opener (pc_race with an opener, 10 s)
# The order alternates: round 1 in the given order, round 2 reversed, and so on.
# Environment: KB, TREE, HERE as for cell.sh; LOAD_MAX (4).
set -u
STORE=$1; OUT=$2; TEST=$3; ROUNDS=$4; shift 4
HERE=${HERE:-$(dirname "$(readlink -f "$0")")}
KB=${KB:-$HERE/kb}; LOAD_MAX=${LOAD_MAX:-4}
ROOT=$STORE/p42-8-micro; MNT=$ROOT/mnt
export HOME_REAL=$HOME
gate() { while ! awk -v m="$LOAD_MAX" '{exit !($1<m)}' /proc/loadavg; do sleep 15; done; }
mount_fs() { # BIN [ENV...]
  local bin=$1; shift
  mkdir -p "$MNT"
  env "$@" HOME=$ROOT/home timeout --kill-after=10s 7200 "$bin" mount / "$MNT" --s3 file://$ROOT/backend \
    --state-dir $ROOT/state --cache-size 4G --foreground > $ROOT/mount.log 2>&1 &
  MPID=$!
  for i in $(seq 1 240); do mountpoint -q "$MNT" && return 0; sleep 0.5; done; return 1
}
umount_fs() {
  fusermount3 -u "$MNT" 2>/dev/null || sudo -n umount -l "$MNT"
  for i in $(seq 1 120); do kill -0 $MPID 2>/dev/null || break; sleep 0.5; done
  kill -0 $MPID 2>/dev/null && { kill $MPID; sleep 2; }
  wait $MPID 2>/dev/null
}
trap 'umount_fs 2>/dev/null; sudo -n rm -rf "$ROOT"' EXIT
r=0
while [ $r -lt "$ROUNDS" ]; do
  r=$((r + 1))
  list=("$@"); if [ $((r % 2)) = 0 ]; then list=(); for ((i=$#; i>0; i--)); do list+=("${!i}"); done; fi
  for v in "${list[@]}"; do
    label=${v%%=*}; rest=${v#*=}; bin=${rest%%,*}; envs=(); [ "$rest" != "$bin" ] && IFS=, read -ra envs <<< "${rest#*,}"
    sudo -n rm -rf "$ROOT"; sudo -n mkdir -p "$ROOT" && sudo -n chown $USER "$ROOT"; mkdir -p $ROOT/home $ROOT/state $ROOT/backend "$MNT"
    HOME=$ROOT/home "$bin" fs create bench --s3 file://$ROOT/backend --chunk-size 1048576 > $ROOT/create.log 2>&1 || { echo "ERROR create $label" >> "$OUT"; continue; }
    gate
    mount_fs "$bin" "${envs[@]}" || { echo "ERROR mount $label" >> "$OUT"; continue; }
    l=$(cut -d' ' -f1 /proc/loadavg)
    case $TEST in
      commit) mkdir "$MNT/c1"; res=$($KB commit "$MNT/c1" - 5000) ;;
      untar) mkdir "$MNT/t1"; TIMEFORMAT='%R'; res="untar_secs=$( { time tar -xf "$TREE" --strip-components=1 -C "$MNT/t1" > /dev/null 2>&1; } 2>&1 )" ;;
      opener) res=$($KB pcrace "$MNT/pc.dat" 10 1) ;;
    esac
    echo "MICRO test=$TEST round=$r label=$label load1=$l $res" >> "$OUT"
    umount_fs
  done
done
echo "MICRO-DONE" >> "$OUT"
