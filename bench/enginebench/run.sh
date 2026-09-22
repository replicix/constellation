#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
unset CARGO_TARGET_DIR
BIN=./target/release/enginebench
OUT=/mnt/enginebench/results.jsonl
DATA=/mnt/enginebench/run
LOG=/mnt/enginebench/run.log
rm -f "$OUT" "$LOG"
mkdir -p "$DATA"

ENTRIES=1000000
THREADS=1,2,4,8,16,32
AGING=5.0
MIXED=45

run() {
  local engine="$1" cache="$2"
  echo "=== $(date -Is) engine=$engine cache_mb=$cache ===" | tee -a "$LOG"
  "$BIN" --engine "$engine" --entries "$ENTRIES" --cache-mb "$cache" \
    --threads "$THREADS" --aging-multiplier "$AGING" --mixed-secs "$MIXED" \
    --data-dir "$DATA" --out "$OUT" >>"$LOG" 2>&1
  echo "=== $(date -Is) done $engine cache_mb=$cache ===" | tee -a "$LOG"
}

run sqlite 256
run sqlite 4096
run sqlite-tuned 4096
run mtree 256
run mtree 4096
run redb 256
run fjall 256
run fjall 4096

echo "ALL DONE $(date -Is)" | tee -a "$LOG"
