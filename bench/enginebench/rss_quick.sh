#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
unset CARGO_TARGET_DIR
BIN=./target/release/enginebench
OUT=/mnt/enginebench/rss_quick.jsonl
DATA=/mnt/enginebench/rssq
LOG=/mnt/enginebench/rss_quick.log
rm -f "$OUT" "$LOG"
mkdir -p "$DATA"

ENTRIES=1000000

run() {
  local engine="$1" cache="$2"
  echo "=== $(date -Is) engine=$engine cache_mb=$cache (quick) ===" | tee -a "$LOG"
  "$BIN" --engine "$engine" --entries "$ENTRIES" --cache-mb "$cache" \
    --threads 1,8,32 --quick \
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

echo "RSS QUICK ALL DONE $(date -Is)" | tee -a "$LOG"
