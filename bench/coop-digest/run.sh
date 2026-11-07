#!/usr/bin/env bash
# Plan 47 P1 benchmark suites. Every job runs under `timeout`.
#   BIN=<path to coop-digest>  OUT=<result dir>  T=<seconds per job>
#   run.sh mem|fp|steady|repair|sim|cuckoo     (correctness / size / wire, any box)
#   run.sh t-lookup|t-steady|t-repair          (timing; quiet box, interleaved)
# Result files are markdown table rows; RESULTS.md quotes them.
set -u
BIN=${BIN:-$(dirname "$0")/target/release/coop-digest}
OUT=${OUT:-$(dirname "$0")/raw}
T=${T:-3600}
COOP_DIGEST_DISK=${COOP_DIGEST_DISK:-${TMPDIR:-/tmp}/coop-digest-disk}
export COOP_DIGEST_DISK
mkdir -p "$OUT" "$COOP_DIGEST_DISK"

job() { # job <file> <args...>
  local f=$1; shift
  echo "# $*" >> "$OUT/$f.md"
  timeout "$T" "$BIN" "$@" >> "$OUT/$f.md" 2>&1 || echo "(exit $? for: $*)" >> "$OUT/$f.md"
}
hdr() { printf '%s\n' "$2" >> "$OUT/$1.md"; }

SPECS_MEM="c1 c6a c6b:4:64 c6b:3:64 c6b:2:64 c3:12:256 c3:16:256 c3:24:256 c3:32:256 c4:14:256 c4:24:256 c5 bloom c8a"
SPECS_MAIN="c1 c6a c6b c3 c4 c5 bloom c8a"
SPECS_FP="c1 c6a c6b:2:64 c6b:3:64 c6b:4:64 c3:12:256 c3:16:256 c3:24:256 c3:32:256 c4:14:256 c4:24:256 c5 bloom c8a"

case "${1:-}" in
mem)
  hdr mem '| candidate | n per peer | peers | mode | live B/key (allocator) | RSS B/key | self-reported B/key | live | RSS delta |'
  for n in 10000 100000 1000000 4000000 8000000; do
    for s in $SPECS_MEM; do job mem mem --spec "$s" --n $n --peers 1; done
  done
  for n in 1000000 8000000; do
    for s in c1 c6a c6b c3 c4 c5 bloom c8a; do job mem mem --spec "$s" --n $n --peers 64; done
  done
  for s in c1 c6a c6b c3 c4 c5; do job mem mem --spec "$s" --n 1000000 --peers 1 --mode sync; done
  job mem mem --spec c1 --n 16000000 --peers 1
  job mem mem --spec c1 --n 16000000 --peers 1 --force
  for n in 16000000 32000000; do
    for s in c6b c3 c4 c5 c8a; do job mem mem --spec "$s" --n $n --peers 1; done
  done
  ;;
fp)
  hdr fp '| candidate | n per peer | peers | hit p50 / p99 ns (fan-out, not cited) | miss p50 / p99 ns | single-peer hit / miss mean ns | false negatives | false positives / lookups | FP rate per lookup per peer |'
  for s in $SPECS_FP; do job fp lookup --spec "$s" --n 1000000 --peers 8 --ops 100000 --fp-ops 1500000; done
  for s in c1 c6a c6b:2:64 c6b:4:64 c3:16:256 c3:32:256 c4:14:256 c5 bloom; do job fp lookup --spec "$s" --n 8000000 --peers 4 --ops 100000 --fp-ops 3000000; done
  for s in c6b:4:64 c6b:3:64 c3:16:256 c3:24:256 bloom; do job fp lookup --spec "$s" --n 16000000 --peers 2 --ops 100000 --fp-ops 6000000; done
  ;;
steady)
  hdr steady '| candidate | n | churn/min | changes per tick | publisher absorb p50 / p99 / mean ns | receiver apply p50 / p99 / mean ns | delta wire B/peer-min | summary wire B/peer-min | snapshot wire B/peer-min | receiver B/key at start / after | publisher set bytes | notes |'
  for n in 1000000 8000000; do
    for c in 0.1 1 10; do
      for s in $SPECS_MAIN; do job steady steady --spec "$s" --n $n --churn $c --minutes 2; done
    done
  done
  for r in 16 256 1024; do job steady steady --spec c5 --n 1000000 --churn 1 --minutes 2 --resnap $r; done
  for r in 16 256 1024; do job steady steady --spec c5 --n 8000000 --churn 1 --minutes 2 --resnap $r; done
  ;;
repair)
  hdr repair '| scenario | candidate | method | result | sessions | rounds | wire bytes | owner / receiver CPU ms | derived wall ms at 1 ms / 50 ms RTT (CPU + rounds x RTT) | false negatives |'
  for n in 1000000 8000000; do
    echo "## n=$n" >> "$OUT/repair.md"
    job repair repair --n $n
  done
  # Where the real session stops converging: gaps between 1k and 100k at 4M and 8M keys.
  for n in 4000000 8000000; do
    echo "## n=$n (gap sweep)" >> "$OUT/repair.md"
    job repair repair --n $n --specs c1,c6b,c3,c4 --scenarios gap10k,gap20k,gap30k,gap50k,gap75k --methods real,rbsr,riblt,replay
  done
  ;;
sim)
  hdr sim '| candidate | n | peers | churn/min | loss / reorder | staleness p50 / p99 / max ms | unresolved | sessions per peer-min | repair B/peer-min | digest wire B/min (deltas+summaries) | stale-positive lookups (removed <=60 s) | missed (held) | false-positive (absent) | final FN | peers not converged | notes |'
  for s in c1 c6a c6b c3 c4 c5 bloom c8a; do
    for loss in 0 0.01 0.1; do
      for rp in 0 5; do job sim sim --spec "$s" --n 100000 --peers 8 --churn 1 --loss $loss --reorder-pct $rp --minutes 10; done
    done
    for c in 0.1 10; do job sim sim --spec "$s" --n 100000 --peers 8 --churn $c --loss 0.01 --reorder-pct 5 --minutes 10; done
    job sim sim --spec "$s" --n 100000 --peers 64 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
    job sim sim --spec "$s" --n 100000 --peers 8 --churn 10 --add-frac 0.75 --loss 0.01 --reorder-pct 5 --minutes 10
  done
  for s in c1 c6b c3 c4; do
    job sim sim --spec "$s" --replay --n 100000 --peers 8 --churn 1 --loss 0.01 --reorder-pct 5 --minutes 10
    job sim sim --spec "$s" --replay --n 100000 --peers 8 --churn 1 --loss 0.1 --reorder-pct 5 --minutes 10
  done
  ;;
cuckoo)
  job cuckoo cuckoo-load
  ;;
fp-large)
  # Rare false positives: five seeds x 50M random absent keys x 4 peers per candidate.
  hdr fp-large '| candidate | n per peer | peers | hit p50 / p99 ns | miss p50 / p99 ns | single-peer hit / miss mean ns | false negatives | false positives / lookups | FP rate per lookup per peer |'
  for s in c6b:4:64 c3:32:256 c3:24:256 c4:24:256; do
    for seed in 1 2 3 4 5; do
      job fp-large lookup --spec "$s" --n 8000000 --peers 4 --ops 1000 --fp-ops 50000000 --seed $seed
    done
  done
  ;;
t-lookup)
  # Timing: interleaved repetitions, order reversed on odd reps (A B B A).
  hdr t-lookup '| rep | candidate | n per peer | peers | hit p50 / p99 ns (fan-out) | miss p50 / p99 ns | single-peer hit / miss mean ns | false negatives | false positives / lookups | FP rate |'
  for cfg in "10000 8" "1000000 8" "1000000 64" "8000000 64"; do
    set -- $cfg; n=$1; p=$2
    for rep in 1 2 3 4; do
      list="c1 c6a c6b c3 c4 c5 bloom c8a"
      [ $((rep % 2)) -eq 0 ] && list=$(echo $list | tr ' ' '\n' | tac | tr '\n' ' ')
      for s in $list; do
        echo -n "| $rep " >> "$OUT/t-lookup.md"
        timeout "$T" "$BIN" lookup --spec "$s" --n $n --peers $p --ops 200000 >> "$OUT/t-lookup.md" 2>&1 || echo "(exit $? $s $n $p)" >> "$OUT/t-lookup.md"
      done
    done
  done
  ;;
t-steady)
  hdr t-steady '| candidate | n | churn/min | changes per tick | publisher absorb p50 / p99 / mean ns | receiver apply p50 / p99 / mean ns | delta wire | summary wire | snapshot wire | B/key | pub bytes | notes |'
  for n in 1000000 8000000; do
    for c in 1 10; do
      for rep in 1 2 3 4; do
        list="c1 c6a c6b c3 c4 c5 bloom c8a"
        [ $((rep % 2)) -eq 0 ] && list=$(echo $list | tr ' ' '\n' | tac | tr '\n' ' ')
        for s in $list; do job t-steady steady --spec "$s" --n $n --churn $c --minutes 2; done
      done
    done
  done
  ;;
t-repair)
  hdr t-repair '| scenario | candidate | method | result | sessions | rounds | wire | owner / receiver CPU ms | derived wall | FN |'
  for rep in 1 2 3 4; do
    list="c1,c6a,c6b,c3,c4,c5,c8a"
    [ $((rep % 2)) -eq 0 ] && list="c8a,c5,c4,c3,c6b,c6a,c1"
    for n in 1000000 8000000; do
      echo "## rep=$rep n=$n" >> "$OUT/t-repair.md"
      sc=gap1,gap1k,gap100k,restart,initial
      [ $n -ge 8000000 ] && sc=gap1,gap1k,gap100k,restart # initial sync at 8M (minutes per run) is timed at 1M only
      job t-repair repair --n $n --specs "$list" --scenarios $sc
    done
  done
  ;;
*)
  echo "usage: run.sh mem|fp|steady|repair|sim|cuckoo|fp-large|t-lookup|t-steady|t-repair" >&2
  exit 2
  ;;
esac
