# Performance Regression Suite

This suite runs Constellation against a **structure-faithful** synthetic tree: an anonymized corpus manifest records hashed path components plus exact file sizes from a real checkout. File bytes are generated on the fly (never stored in git). It then compares branch vs base in three network profiles:

- `full` (no shaping)
- `latency250` (250 ms latency via toxiproxy)
- `bw50mbps` (50 Mbps bandwidth via toxiproxy)

## Corpus

`tests/perf_regression/corpus.jsonl.zst` is a zstd JSONL manifest: one metadata line, then one record per directory and file. Path components are keyed-BLAKE3 tokens (seed 42). Each file stores **exact size**. While snapshotting, original bytes are hashed only in memory so duplicates can be detected; those hashes are **not** written out. Files that shared content get the same small integer `i`; unique files omit `i`. Replay fills a file from that id (or from the anonymized path, if unique), so duplicates stay byte-identical and still exercise whole-file dedup. `.git` / `.hg` / `.svn` are skipped; symlinks are omitted.

Regenerate after walking a local tree (names never enter git):

```bash
cargo run -p constellation-harness --release -- corpus-snapshot \
  --src /path/to/tree \
  --out tests/perf_regression/corpus.jsonl.zst \
  --seed 42
```

`make perf-regression` and the GitHub workflow pass `--corpus-shape`, which replays that bundled manifest. Optional workflow inputs / flags:

- `--corpus-limit N` — replay only the first N files
- `--max-file-bytes N` — cap each staged file's payload (directory shape unchanged)

CI defaults to `--max-file-bytes 2097152` (~2 MiB) so the full tree shape fits on a standard GitHub runner (exact sizes are ~9 GB and do not). Clear that input on `workflow_dispatch` for an uncapped local-scale run on a larger machine.

Flat `--files` / `--fanout` / `--file-size` generation remains available when corpus mode is off.

## Comparison with main

Every push to `main` runs the suite and keeps `head.json` in the `perf-results` artifact (90 days). A pull request runs the suite once, downloads the newest of those, and renders the comparison with `report.py`: a geometric-mean overview per network profile, then every metric against main. Changes under ±15% are shown in small print (hosted runners are that noisy); a main measured with a different corpus (seed, file limit, size cap) is not compared. The report is in the job summary and in one comment per pull request, updated in place. Nothing is committed to the branch.

Artifacts per run (14 days): `perf-{run_id}` with `head.json`, `report.md` and the raw constellation/harness logs.

## Local run

```bash
make perf-regression
```

## Compare two suites (Markdown to stdout)

```bash
python3 tests/perf_regression/report.py head.json --base base.json
```
