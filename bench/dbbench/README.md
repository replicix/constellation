# dbbench

Standalone bake-off for Constellation's local metadata engine (ADR-9).

## Setup

```bash
# Vacuum a live mount's meta.db into the corpus (example path):
sqlite3 ~/.local/share/constellation/<fs-id>/meta.db \
  "VACUUM INTO '$(pwd)/data/corpus-src.db'"

cargo build --release
./target/release/dbbench --threads 8
./target/release/dbbench rocksdb --threads 32
```

Reports isolated throughput, full latency distributions (p50…p99.9/max), and a
15 s concurrent mixed workload (lookup/getattr/readdir/setattr) with 100 ms
window stall detection. See `RESULTS.md`.
