#!/bin/bash
# Plan 42 section 8: every benchmark, on one build and one backing store (a "cell").
# usage: cell.sh BIN STOREDIR TAG OUT [fast]
#   BIN       constellation binary (base or final build)
#   STOREDIR  directory on the backing store; the cell's backend, state dir and cache go in STOREDIR/p42-8-TAG
#   OUT       result file (RESULT / LOAD lines are appended)
# Environment: KB (the compiled kb), TREE (tree.tar from gen-tree.py), HERE (this directory),
#   LOAD_MAX (default 4), PHASE_WAIT (seconds to wait for a phase's load gate, default 900),
#   FINAL=1 for the final build (adds the W1/W2 variants of the pc_race rows), FAST=1 for a smoke run.
set -u
BIN=$1; STORE=$2; TAG=$3; OUT=$4
HERE=${HERE:-$(dirname "$(readlink -f "$0")")}
KB=${KB:-$HERE/kb}
LOAD_MAX=${LOAD_MAX:-4}
PHASE_WAIT=${PHASE_WAIT:-900}
FAST=${FAST:-0}
ROOT=$STORE/p42-8-$TAG
MNT=$ROOT/mnt; MNT2=$ROOT/mnt2
MPID=""
export HOME_REAL=$HOME

say() { echo "$@" >> "$OUT"; }
load1() { cut -d' ' -f1 /proc/loadavg; }
# gate: wait until the 1-minute load is below LOAD_MAX; records the load seen and whether the wait gave up
gate() { # NAME
  local t0=$SECONDS l ok=1
  while :; do
    l=$(load1)
    if awk -v l="$l" -v m="$LOAD_MAX" 'BEGIN{exit !(l<m)}'; then break; fi
    if [ $((SECONDS - t0)) -ge "$PHASE_WAIT" ]; then ok=0; break; fi
    sleep 15
  done
  say "LOAD phase=$1 load1=$l waited=$((SECONDS - t0)) gate_ok=$ok"
}
mount_fs() { # [ENV=VAL ...]  : mount view 1 at MNT
  mkdir -p "$MNT"
  env "$@" HOME=$ROOT/home timeout --kill-after=10s 7200 "$BIN" mount / "$MNT" --s3 file://$ROOT/backend \
    --state-dir $ROOT/state --cache-size ${CACHE:-4G} --foreground > $ROOT/mount.log 2>&1 &
  MPID=$!
  local i
  for i in $(seq 1 240); do mountpoint -q "$MNT" && return 0; sleep 0.5; done
  say "ERROR mount failed"; tail -5 $ROOT/mount.log >> "$OUT"; return 1
}
mount_view2() {
  mkdir -p "$MNT2"
  HOME=$ROOT/home timeout --kill-after=10s 120 "$BIN" mount / "$MNT2" --s3 file://$ROOT/backend --state-dir $ROOT/state > $ROOT/mount2.log 2>&1
  mountpoint -q "$MNT2" || { say "ERROR second view failed"; cat $ROOT/mount2.log >> "$OUT"; return 1; }
}
umount_fs() {
  mountpoint -q "$MNT2" 2>/dev/null && { fusermount3 -u "$MNT2" 2>/dev/null || sudo -n umount -l "$MNT2"; }
  fusermount3 -u "$MNT" 2>/dev/null || sudo -n umount -l "$MNT"
  local i
  for i in $(seq 1 120); do kill -0 $MPID 2>/dev/null || break; sleep 0.5; done
  if kill -0 $MPID 2>/dev/null; then kill $MPID; sleep 2; fi
  wait $MPID 2>/dev/null
}
# FRESH=1 (tmpfs, whose space is the host's memory): a new, empty file system before the phases that write
# much, so the backend of one phase is not kept for the next
newfs() {
  mkdir -p $ROOT/home $ROOT/state $ROOT/backend $MNT $MNT2
  HOME=$ROOT/home "$BIN" fs create bench --s3 file://$ROOT/backend --chunk-size 1048576 > $ROOT/create.log 2>&1 || { say "ERROR fs create"; cat $ROOT/create.log >> "$OUT"; return 1; }
}
wipefs_() { local i; for i in 1 2 3 4 5 6; do rm -rf $ROOT/home $ROOT/state $ROOT/backend; [ -e $ROOT/state ] || [ -e $ROOT/backend ] || return 0; sleep 2; done; }
fresh() {
  [ "${FRESH:-0}" = 1 ] || return 0
  umount_fs
  wipefs_
  newfs && mount_fs
}
dropc() { sudo -n sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'; }
snap() { python3 $HERE/snap.py "$BIN" $ROOT/state 2>/dev/null; }
# time a command: prints "wall user sys" into $TM; the command's own output is discarded
tm() {
  local TIMEFORMAT='%R %U %S'
  TM=$( { time "$@" > /dev/null 2>&1; } 2>&1 )
}
# measure NAME cmd...  : RESULT line with wall/cpu and the request counts it caused
measure() { # NAME CMD...
  local name=$1; shift
  snap > $ROOT/s0.json
  tm "$@"
  snap > $ROOT/s1.json
  say "RESULT $name time=\"$TM\" $(python3 $HERE/snap.py --diff $ROOT/s0.json $ROOT/s1.json)"
}
phase() { gate "$1"; say "PHASE $1 start $(date +%s)"; }

cleanup() { kill $SAMPLER 2>/dev/null; umount_fs 2>/dev/null; sudo -n rm -rf "$ROOT"; }
trap cleanup EXIT

python3 $HERE/sampler.py "$OUT.samples" "$(ps -o sid= -p $$ | tr -d ' ')" &
SAMPLER=$!
say "CELL tag=$TAG bin=$(basename $BIN) store=$STORE kernel=$(uname -r) start=$(date +%FT%T)"
sudo -n rm -rf "$ROOT"; sudo -n mkdir -p "$ROOT" && sudo -n chown $USER "$ROOT"
mkdir -p $ROOT/home $ROOT/state $ROOT/backend $MNT $MNT2
HOME=$ROOT/home "$BIN" fs create bench --s3 file://$ROOT/backend --chunk-size 1048576 > $ROOT/create.log 2>&1 || { say "ERROR fs create"; cat $ROOT/create.log >> "$OUT"; exit 1; }

# ---- 1. untar of the tree, cp -a of it
phase write
mount_fs || exit 1
say "STATUS $(snap | head -c 400)"
mkdir "$MNT/t1"
measure untar tar -xf "$TREE" --strip-components=1 -C "$MNT/t1"
measure cp_a cp -a "$MNT/t1" "$MNT/t2"
git -C "$MNT/t1" update-index -q --refresh >/dev/null 2>&1; git -C "$MNT/t1" status --porcelain > /dev/null 2>&1
sync
umount_fs

# ---- 2. read workloads on the tree, kernel-cold then warm (fresh mount each, caches dropped)
phase tree
mount_fs || exit 1
t1=$MNT/t1; t2=$MNT/t2
declare -A W
W[ls_lR]="ls -lR $t1"
W[find_f]="find $t1 -type f"
W[git_status]="git -C $t1 status --porcelain"
W[make_q]="make -q -C $t1 all"
W[rsync_n]="rsync -an --delete $t1/ $t2/"
for w in ls_lR find_f git_status make_q rsync_n; do
  [ $FAST = 1 ] && [ $w != git_status ] && continue
  dropc
  measure ${w}_cold ${W[$w]}
  measure ${w}_warm ${W[$w]}
done
umount_fs

# ---- 3. reopen-read, open latency, fio, database-style, commit-heavy, pc_race without opener
phase misc
if [ "${FRESH:-0}" = 1 ]; then wipefs_; newfs || exit 1; fi
mount_fs || exit 1
NF=5000; [ $FAST = 1 ] && NF=500
$KB reopen-prep "$MNT/ro" $NF 16384 >> /dev/null
sync
umount_fs
mount_fs || exit 1
dropc
for pass in 1 2 3; do
  snap > $ROOT/s0.json
  line=$($KB reopen-pass "$MNT/ro" $NF 16384)
  snap > $ROOT/s1.json
  say "$line pass=$pass $(python3 $HERE/snap.py --diff $ROOT/s0.json $ROOT/s1.json)"
done
say "KEEPCACHE $(python3 $HERE/snap.py --fuse $ROOT/s1.json)"
rm -rf "$MNT/ro"

if [ "${FRESH:-0}" = 1 ]; then f1=$MNT/ol.dat; head -c 8192 /dev/zero | tr '\0' 'x' > "$f1"; else f1=$(find "$MNT/t1/a00/b0/c0" -name 'f00.c' | head -1); fi
cat "$f1" > /dev/null
ON=100000; [ $FAST = 1 ] && ON=5000
snap > $ROOT/s0.json
say "$($KB openlat "$f1" $ON) $(snap > $ROOT/s1.json; python3 $HERE/snap.py --diff $ROOT/s0.json $ROOT/s1.json)"

fresh
phase fio
mkdir -p "$MNT/fio"
FS=256M; RT=20; [ $FAST = 1 ] && { FS=16M; RT=3; }
for job in "seqwrite write 1M" "randwrite randwrite 4k" "randrw randrw 4k"; do
  set -- $job
  extra=""; [ $2 = randrw ] && extra="--rwmixread=70"
  [ $2 = write ] && extra="--end_fsync=1" || extra="$extra --time_based --runtime=$RT"
  snap > $ROOT/s0.json
  fio --name=$1 --directory="$MNT/fio" --numjobs=4 --size=$FS --rw=$2 --bs=$3 --ioengine=psync \
      --group_reporting --output-format=json --output=$ROOT/fio-$1.json --ramp_time=0 $extra > /dev/null 2>&1
  snap > $ROOT/s1.json
  say "RESULT fio_$1 $(python3 $HERE/fio_sum.py $ROOT/fio-$1.json) $(python3 $HERE/snap.py --diff $ROOT/s0.json $ROOT/s1.json)"
  rm -f "$MNT"/fio/*
done
rm -rf "$MNT/fio"

fresh
phase db
DBB=1073741824; DT=60; [ $FAST = 1 ] && { DBB=67108864; DT=3; }
$KB dbprep "$MNT/db.dat" $DBB > /dev/null
snap > $ROOT/s0.json
r=$($KB db "$MNT/db.dat" $DT 4)
snap > $ROOT/s1.json
say "$r $(python3 $HERE/snap.py --diff $ROOT/s0.json $ROOT/s1.json)"
rm -f "$MNT/db.dat"

fresh
phase commit
CN=5000; [ $FAST = 1 ] && CN=300
mkdir -p "$MNT/c1" "$MNT/c1v"
snap > $ROOT/s0.json
r=$($KB commit "$MNT/c1" - $CN); snap > $ROOT/s1.json
say "$r views=1 $(python3 $HERE/snap.py --diff $ROOT/s0.json $ROOT/s1.json) $(python3 $HERE/snap.py --fuse $ROOT/s1.json)"
if mount_view2; then
  mkdir -p "$MNT/c2"
  snap > $ROOT/s0.json
  r=$($KB commit "$MNT/c2" "$MNT2/c2" $CN); snap > $ROOT/s1.json
  say "$r views=2 $(python3 $HERE/snap.py --diff $ROOT/s0.json $ROOT/s1.json) $(python3 $HERE/snap.py --fuse $ROOT/s1.json)"
fi

fresh
phase pcrace
PS=20; [ $FAST = 1 ] && PS=3
snap > $ROOT/s0.json
r=$($KB pcrace "$MNT/pc.dat" $PS 0); snap > $ROOT/s1.json
say "$r variant=default_noopener $(python3 $HERE/snap.py --diff $ROOT/s0.json $ROOT/s1.json) $(python3 $HERE/snap.py --fuse $ROOT/s1.json)"
rm -f "$MNT/pc.dat"
umount_fs

# ---- 4. pc_race with an opener (the repair's worst case needs KEEP_CACHE=off)
pcvariant() { # LABEL ENV...
  local label=$1; shift
  phase pc_$label
  mount_fs "$@" || return 1
  snap > $ROOT/s0.json
  local r; r=$($KB pcrace "$MNT/pc.dat" $PS 1); snap > $ROOT/s1.json
  say "$r variant=$label $(python3 $HERE/snap.py --diff $ROOT/s0.json $ROOT/s1.json) $(python3 $HERE/snap.py --fuse $ROOT/s1.json)"
  umount_fs
}
pcvariant opener_default
pcvariant opener_keepoff CONSTELLATION_FUSE_KEEP_CACHE=off
if [ "${FINAL:-0}" = 1 ]; then
  pcvariant opener_keepoff_repairoff CONSTELLATION_FUSE_KEEP_CACHE=off CONSTELLATION_FUSE_WRITE_REPAIR=off
fi
say "CELL-END tag=$TAG $(date +%FT%T) epoch=$(date +%s)"
