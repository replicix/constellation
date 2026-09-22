#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
unset CARGO_TARGET_DIR
BIN=./target/release/enginebench
OUT=/mnt/enginebench/large_scale.jsonl
DATA=/mnt/enginebench/large
LOG=/mnt/enginebench/large_scale.log
rm -f "$OUT" "$LOG"
mkdir -p "$DATA"

# 5M entries, ~15M keys. aging_multiplier=1.0 -> 15M mutations, the same
# absolute mutation count as the primary (1M-entry, 5x) suite, so the
# per-mutation wall-clock cost is directly comparable and the run stays
# inside the time budget; this is a *smaller relative* aging pass (1x
# key count vs the primary regime's 5x) purely to confirm the fresh vs
# aged trend and the cache<<DB regime persist at 5x the corpus, not to
# repeat the full aging sweep.
ENTRIES=5000000
THREADS=1,2,4,8,16,32
AGING=1.0
MIXED=20
CACHE=256

run() {
  local engine="$1"
  echo "=== $(date -Is) engine=$engine cache_mb=$CACHE entries=$ENTRIES ===" | tee -a "$LOG"
  "$BIN" --engine "$engine" --entries "$ENTRIES" --cache-mb "$CACHE" \
    --threads "$THREADS" --aging-multiplier "$AGING" --mixed-secs "$MIXED" \
    --data-dir "$DATA" --out "$OUT" >>"$LOG" 2>&1
  echo "=== $(date -Is) done $engine ===" | tee -a "$LOG"
}

run sqlite
run mtree
run fjall

echo "LARGE SCALE DONE $(date -Is)" | tee -a "$LOG"
