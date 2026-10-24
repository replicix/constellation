#!/bin/bash
# The plan 40 section 7 xcheckfs runs, one after another: SECS each (default one hour).
# Needs the scripts of this directory and bench/readdirplus/gen-tree.py (as gen-tree.py, A=2 for the 5,000-file
# repository: change `range(20)` to `range(2)`) in $H (default ~/cst-p40p9-xc), a constellation build (CST_BIN), an
# xcheckfs build (XC) and an ext4 file system at /mnt/ext4 for the primary and the work directories.
H=${H:-$HOME/cst-p40p9-xc}
SECS=${SECS:-3600}
for cfg in "auto 1 auto-n1" "off 1 off-n1" "auto 2 auto-n2" "off 2 off-n2"; do
  set -- $cfg
  $H/xc-run.sh $1 $2 $SECS $3 > $H/logs/run-$3.out 2>&1
  echo "$(date +%T) $3 done rc=$?" >> $H/logs/all.log
done
echo ALLDONE >> $H/logs/all.log
