#!/bin/bash
# The sim determinism gate: `seed -> fingerprint` is a pure function, so the
# same labels and seeds at two thread counts give identical fingerprint files
# and identical failing sets. Run from the repository root:
#
#   tools/sim-determinism-gate.sh [SEEDS] [OUT_DIR]
#
# Needs a release `sim` test binary: it is built here. SEEDS defaults to 300.
# THREADS_A / THREADS_B (default 2 / 16) are the two arms; PAR_A / PAR_B
# (default 8 / 3) the labels run in parallel in each; TIMEOUT (default 3600 s)
# bounds every sweep. Exit 0 when the arms agree.
set -u
SEEDS=${1:-300}
OUT=${2:-$(mktemp -d)}
THREADS_A=${THREADS_A:-2}; THREADS_B=${THREADS_B:-16}
PAR_A=${PAR_A:-8}; PAR_B=${PAR_B:-3}
TIMEOUT=${TIMEOUT:-3600}
LABELS=${LABELS:-"flex+links backup-crash+links backup-crash delegated-holder-cut+links metered-shared+links placement-hot+links locks-core-stall-backup-long delegated-crash+links"}
mkdir -p "$OUT"
ulimit -n 524288 2>/dev/null
BIN=$(cargo test --release -p constellation-authority --test sim --no-run --message-format=json 2>/dev/null |
  sed -n 's/.*"executable":"\([^"]*\)".*/\1/p' | tail -1)
[ -x "$BIN" ] || { echo "no sim test binary" >&2; exit 2; }

arm() { # arm NAME THREADS PARALLEL
  local name=$1 th=$2 par=$3
  mkdir -p "$OUT/$name"
  one() {
    AUTHORITY_SIM_FINGERPRINT=1 AUTHORITY_SIM_FP_FILE="$OUT/$name/$1.fp" \
      AUTHORITY_SIM_CONFIG=$1 AUTHORITY_SIM_START=0 AUTHORITY_SIM_SEEDS=$SEEDS \
      AUTHORITY_SIM_THREADS=$th timeout "$TIMEOUT" "$BIN" --ignored --exact sweep_config --nocapture 2>&1 |
      grep -a '^SWEEP-FAIL' | sed 's/^\(SWEEP-FAIL [^ ]* seed [0-9]*\).*/\1/' | sort > "$OUT/$name/$1.fail"
    sort -o "$OUT/$name/$1.fp" "$OUT/$name/$1.fp"
  }
  export -f one; export BIN OUT SEEDS name th TIMEOUT
  printf '%s\n' $LABELS | xargs -P "$par" -I{} bash -c 'one {}'
}
arm a "$THREADS_A" "$PAR_A"
arm b "$THREADS_B" "$PAR_B"
rc=0
for l in $LABELS; do
  n=$(wc -l < "$OUT/a/$l.fp")
  [ "$n" -eq "$SEEDS" ] || { echo "$l: arm a has $n of $SEEDS fingerprints"; rc=1; }
  cmp -s "$OUT/a/$l.fp" "$OUT/b/$l.fp" || { echo "$l: fingerprints differ: $(diff "$OUT/a/$l.fp" "$OUT/b/$l.fp" | grep -c '^<') seeds"; rc=1; }
  cmp -s "$OUT/a/$l.fail" "$OUT/b/$l.fail" || { echo "$l: failing sets differ"; rc=1; }
done
[ $rc -eq 0 ] && echo "determinism gate: $(echo $LABELS | wc -w) labels x $SEEDS seeds identical at $THREADS_A and $THREADS_B threads"
exit $rc
