# Performance Regression Suite

This suite runs Constellation against a synthetic dataset generated **on the fly** (never stored in git), then compares branch vs base in three network profiles:

- `full` (no shaping)
- `latency250` (250 ms latency via toxiproxy)
- `bw50mbps` (50 Mbps bandwidth via toxiproxy)

## Baseline storage strategy

Performance baselines are committed directly to each branch:

### `.perf-baselines/result.json` in branch
- Each branch maintains its own baseline
- File path: `.perf-baselines/result.json`
- Updated automatically after successful benchmark runs
- Committed with `[skip ci]` to avoid triggering loops

### How it works
1. PR opens against base branch (e.g., `main`)
2. Workflow fetches baseline from base branch (`.perf-baselines/result.json`)
3. If not found: runs benchmark for base commit to generate baseline
4. Runs benchmark for PR head
5. Compares head vs base and generates report
6. Commits head's baseline to PR branch
7. When PR merges, updated baseline flows to base branch

### Benefits
- ✅ Indefinite retention (follows git history)
- ✅ Intuitive: baseline lives with the code it measures
- ✅ Works with any ref (branches, tags, SHAs)
- ✅ Self-contained: no separate branch management
- ✅ Fast: existing baselines skip expensive rebuilds
- ✅ Automatic: baselines update on every PR merge

### Artifacts (ephemeral)

Short-lived workflow artifacts for recent runs only:

- **Comparison reports** (14 days): `perf-report-{run_id}` - Markdown diff tables
- **Logs** (14 days): `perf-logs-{run_id}` - Raw constellation/harness logs for debugging

## Local run

```bash
make perf-regression
```

## Compare two suites

```bash
python3 tests/perf_regression/compare.py --base base.json --head head.json --out report.md
```
