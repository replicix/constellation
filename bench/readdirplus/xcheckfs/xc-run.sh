#!/bin/bash
# xc-run.sh MODE NODES SECS TAG
#   MODE   CONSTELLATION_FUSE_READDIRPLUS for the constellation mounts: off | auto
#   NODES  1 or 2: with 2, a second daemon mounts the same store and runs the same mix in a sibling directory,
#          and loops ls -lR / find -ls over the first node's tree (remote changes invalidate plus-installed dentries)
#   SECS   how long the workload runs under the xcheckfs mount
# Layout: primary = ext4 loop file system, secondary = <work>/mnt/xc (a constellation mount, file store); xcheckfs
# mounts over both at <work>/xcmnt. Findings: xcheckfs mismatches, plus the differences of `xcheckfs verify` before
# and after. Work directories: /mnt/ext4/cst-p40p9-w/TAG; MAKE_GOLDEN=1 saves the populated file system to /mnt/ext4/cst-p40p9-gold,
# which later runs copy instead of populating (about 10 minutes for 200,000 files).
MODE=$1; NODES=$2; SECS=$3; TAG=$4
H=$HOME/cst-p40p9-xc
BIN=${CST_BIN:-$HOME/cst-p40p9-target/release/constellation}
XC=${XC:-$HOME/cst-p40p9-xcheckfs-target/release/xcheckfs}
EXT4=/mnt/ext4
W=/mnt/ext4/cst-p40p9-w/$TAG; L=$H/logs/$TAG; mkdir -p $W $L
P=$EXT4/cst-p40p9-$TAG
S=$W/store; MNT1=$W/mnt1; MNT2=$W/mnt2; XM=$W/xcmnt
mkdir -p $S $MNT1 $MNT2 $XM $P
say() { echo "$(date +%T) $*" | tee -a $L/run.log; }
up() { # N : mount node N
  local st=$W/state$1 m=$W/mnt$1
  mkdir -p $st
  (cd $W; CONSTELLATION_FUSE_READDIRPLUS=$MODE setsid nohup timeout --kill-after=60s $((SECS + 7200)) $BIN mount / $m --s3 $S --state-dir $st --foreground >> $L/daemon$1.log 2>&1 < /dev/null & echo $! > $W/daemon$1.pid)
  for i in $(seq 120); do mountpoint -q $m && break; sleep 1; done
  mountpoint -q $m || { say "node $1 mount FAILED"; tail -20 $L/daemon$1.log; exit 1; }
}
down() {
  local m=$W/mnt$1
  fusermount3 -u $m 2>/dev/null || { sleep 2; fusermount3 -uz $m; }
  local p; p=$(cat $W/daemon$1.pid 2>/dev/null)
  for i in $(seq 90); do [ -n "$p" ] && kill -0 $p 2>/dev/null || break; sleep 1; done
  [ -n "$p" ] && kill -0 $p 2>/dev/null && { say "daemon $p still alive: TERM"; kill $p; sleep 5; }
}
GOLD=/mnt/ext4/cst-p40p9-gold
if [ -d $GOLD/store ] && [ -z "$MAKE_GOLDEN" ]; then
  # the populated filesystem of an earlier run: store, node 1's state and the primary tree
  cp -a $GOLD/store/. $S/ && cp -a $GOLD/state1 $W/state1 && cp -a $GOLD/primary/. $P/ && POPULATED=1
else
  $BIN fs create p40p9-$TAG --s3 $S > $L/fscreate.log 2>&1 || { cat $L/fscreate.log; exit 1; }
fi
up 1
[ "$NODES" = 2 ] && up 2
say "mounted: mode=$MODE nodes=$NODES kernel=$(uname -r) uring=$(cat /sys/module/fuse/parameters/enable_uring)"
SEC=$MNT1/xc; mkdir -p $SEC
# the trees: 200,000 files on the primary, mirrored to the secondary, verified
[ -d $H/tree200k ] || python3 $H/gentree200k.py $H/tree200k > /dev/null
[ -d $H/tree5k ] || { mkdir -p $H/tree5k && cp -al $H/tree200k/d000? $H/tree200k/d001? $H/tree200k/d002? $H/tree200k/d003? $H/tree200k/d004? $H/tree5k/; }
[ -d $H/srcrepo ] || { python3 $H/gen-tree.py $H/srcrepo > /dev/null; git -C $H/srcrepo checkout -q -b alt; for f in $(git -C $H/srcrepo ls-files 'a0*/b*/c*/f0[0-4].c' | head -3000); do echo "// alt" >> $H/srcrepo/$f; done; git -C $H/srcrepo -c user.name=b -c user.email=b@x commit -q -am alt; git -C $H/srcrepo checkout -q -; }
if [ -n "$POPULATED" ]; then
  touch -r $P/tree $SEC/tree   # the parallel rsync does not set the container's mtime
fi
if [ -z "$POPULATED" ]; then
  mkdir -p $P/tree $SEC/tree && rsync -aH --numeric-ids $H/tree200k/ $P/tree/ && ls $P/tree | xargs -P 8 -I{} rsync -aH --numeric-ids $P/tree/{} $SEC/tree/
fi
say "pre-flight verify: $($XC verify $P $SEC 2>&1 | tail -3 | tr '\n' ' ')"
if [ -n "$MAKE_GOLDEN" ]; then
  say "golden: stopping to save the populated filesystem"
  [ "$NODES" = 2 ] && down 2
  down 1
  rm -rf $GOLD; mkdir -p $GOLD/primary
  cp -a $S $GOLD/store && cp -a $W/state1 $GOLD/state1 && cp -a $P/. $GOLD/primary/
  say "golden saved"
  exit 0
fi
setsid nohup $XC mount -c basic -m log -l warn --no-color --log-file $L/xcheckfs.log $XM $P $SEC > $L/xcheckfs.stdout 2>&1 < /dev/null &
XP=$!; echo $XP > $W/xc.pid
for i in $(seq 60); do mountpoint -q $XM && break; sleep 1; done
mountpoint -q $XM || { say "xcheckfs mount FAILED"; cat $L/xcheckfs.stdout; kill $XP; exit 1; }
(while :; do echo "$(date +%s) $(cut -d' ' -f1 /proc/loadavg)"; sleep 20; done > $L/load.txt) & LS=$!
if [ "$NODES" = 2 ]; then
  mkdir -p $MNT2/sibling
  ( $H/xc-workload.sh $MNT2/sibling $SECS $L/workload2.log $H/tree5k $H/srcrepo ) &
  W2=$!
  ( end=$(( $(date +%s) + SECS )); while [ $(date +%s) -lt $end ]; do ls -lR $MNT2/xc > /dev/null 2>&1; find $MNT2/xc -maxdepth 3 -ls > /dev/null 2>&1; done ) &
  R2=$!
fi
say "workload starts ($SECS s)"
$H/xc-workload.sh $XM/work $SECS $L/workload1.log $H/tree5k $H/srcrepo
say "workload done"
[ "$NODES" = 2 ] && { wait $W2; kill $R2 2>/dev/null; wait $R2 2>/dev/null; }
kill $LS 2>/dev/null
$XC ctl $XM status > $L/ctl-status.txt 2>&1
$XC ctl $XM mismatches 200 > $L/ctl-mismatches.json 2>&1
$BIN status / --state-dir $W/state1 > $L/status1.json 2>&1
[ "$NODES" = 2 ] && $BIN status / --state-dir $W/state2 > $L/status2.json 2>&1
fusermount3 -u $XM || { sleep 3; fusermount3 -uz $XM; }
for i in $(seq 60); do kill -0 $XP 2>/dev/null || break; sleep 1; done
kill -0 $XP 2>/dev/null && { kill $XP; sleep 3; }
say "post verify: $($XC verify $P $SEC 2>&1 | tail -5 | tr '\n' ' ')"
[ "$NODES" = 2 ] && down 2
down 1
say "xcheckfs ctl status: $(tr '\n' ' ' < $L/ctl-status.txt | cut -c1-400)"
say "steps failing: $(grep -v 'rc=0' $L/workload1.log | grep -c rc=)"
rm -rf $W/store $W/state1 $W/state2
