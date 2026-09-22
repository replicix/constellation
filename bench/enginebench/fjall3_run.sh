#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
unset CARGO_TARGET_DIR
BIN=./target/release/enginebench
OUT=/mnt/enginebench/fjall3.jsonl
DATA=/mnt/enginebench/fjall3run
LOG=/mnt/enginebench/fjall3.log
rm -f "$OUT" "$LOG"
mkdir -p "$DATA"

THREADS=1,2,4,8,16,32

run_1m() {
  local engine="$1"
  echo "=== $(date -Is) engine=$engine entries=1000000 cache_mb=256 ===" | tee -a "$LOG"
  "$BIN" --engine "$engine" --entries 1000000 --cache-mb 256 \
    --threads "$THREADS" --aging-multiplier 5.0 --mixed-secs 45 \
    --data-dir "$DATA" --out "$OUT" >>"$LOG" 2>&1
  echo "=== $(date -Is) done $engine (1M) ===" | tee -a "$LOG"
}

run_5m() {
  local engine="$1"
  echo "=== $(date -Is) engine=$engine entries=5000000 cache_mb=256 ===" | tee -a "$LOG"
  "$BIN" --engine "$engine" --entries 5000000 --cache-mb 256 \
    --threads "$THREADS" --aging-multiplier 1.0 --mixed-secs 20 \
    --data-dir "$DATA" --out "$OUT" >>"$LOG" 2>&1
  echo "=== $(date -Is) done $engine (5M) ===" | tee -a "$LOG"
}

# 1M-entry primary matrix, 256 MiB, matching the original fjall/fjall-tuned params exactly.
run_1m fjall3
run_1m fjall3-tuned

# 5M-entry confirmation, 256 MiB, matching large_scale.sh params exactly.
run_5m fjall3
# Rerun v2 fjall at 5M/256 too, same session, for a fair v2-vs-v3 comparison
# in case machine state shifted since the original large_scale.sh run.
run_5m fjall

echo "FJALL3 RUN DONE $(date -Is)" | tee -a "$LOG"
