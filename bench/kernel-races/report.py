#!/usr/bin/env python3
"""Plan 42 section 8: the RESULTS.md tables from cell files. usage: report.py OUTDIR [MICRODIR]"""
import glob, os, re, statistics, sys
sys.argv = [sys.argv[0]] + sys.argv[1:]
src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "agg.py")).read().replace("\nmain()\n", "\n")
ns = {}
exec(compile(src, "agg", "exec"), ns)
metrics_of, higher_better, STORES = ns["metrics_of"], ns["higher_better"], ns["STORES"]
out = sys.argv[1]
# The first tmpfs attempt ran out of space (16 GiB of the host's memory) from the fio phase on; its later phases are
# not results. --tmpfs-all keeps them (for cells run with FRESH=1, which start the writing phases on an empty file system).
TMPFS_SKIP = () if "--tmpfs-all" in sys.argv else ("fio_", "db.", "commit.", "pcrace.")
data = {}
for p in sorted(glob.glob(os.path.join(out, "cell-*.txt"))):
    mt = re.match(r"cell-(.+)-(base|final)-r(\d+)\.txt$", os.path.basename(p))
    if not mt or "CELL-END" not in open(p).read():
        continue
    m, loads, errors = metrics_of(p)
    d = data.setdefault((mt.group(1), mt.group(2)), {})
    for k, v in m.items():
        if mt.group(1) == "tmpfs" and k.startswith(TMPFS_SKIP):
            continue
        d.setdefault(k, []).append(v)

def stat(store, ver, k):
    v = data.get((store, ver), {}).get(k)
    if not v:
        return None
    med = statistics.median(v)
    return med, ((max(v) - min(v)) / med if med else 0.0), len(v)

def cell(store, k):
    a, b = stat(store, "base", k), stat(store, "final", k)
    if not a or not b or not a[0]:
        return "n/a"
    w = (a[0] / b[0] if b[0] else float("inf")) if higher_better(k) else b[0] / a[0]
    flag = "*" if w - 1 > max(0.03, 2 * a[1]) else ""
    return f"{w:.2f}{flag}"

ROWS = [
    ("untar 50k files", "untar.wall_s"), ("cp -a of it", "cp_a.wall_s"),
    ("ls -lR cold", "ls_lR_cold.wall_s"), ("ls -lR warm", "ls_lR_warm.wall_s"),
    ("find cold", "find_f_cold.wall_s"), ("find warm", "find_f_warm.wall_s"),
    ("git status cold", "git_status_cold.wall_s"), ("git status warm", "git_status_warm.wall_s"),
    ("make -q cold", "make_q_cold.wall_s"), ("make -q warm", "make_q_warm.wall_s"),
    ("rsync -n cold", "rsync_n_cold.wall_s"), ("rsync -n warm", "rsync_n_warm.wall_s"),
    ("reopen-read pass 1", "reopen_pass.p1.secs"), ("reopen-read pass 2", "reopen_pass.p2.secs"),
    ("reopen-read pass 3", "reopen_pass.p3.secs"),
    ("open+close mean", "openlat.open_close_mean_ns"), ("open+close p50", "openlat.open_close_p50_ns"),
    ("open+close p99", "openlat.open_close_p99_ns"),
    ("fio seq write 1M, MiB/s", "fio_seqwrite.write_bw_kib"), ("fio rand write 4k, IOPS", "fio_randwrite.write_iops"),
    ("fio rand rw 70/30, read IOPS", "fio_randrw.read_iops"), ("fio rand rw 70/30, write IOPS", "fio_randrw.write_iops"),
    ("pc_race no opener, writes", "pcrace.default_noopener.writes"),
    ("pc_race no opener, write p50", "pcrace.default_noopener.write_p50_ns"),
    ("pc_race no opener, write p99", "pcrace.default_noopener.write_p99_ns"),
    ("db 4 threads, ops/s", "db.ops_per_sec"), ("db read p50", "db.read_p50_ns"), ("db read p99", "db.read_p99_ns"),
    ("db write p50", "db.write_p50_ns"), ("db write p99", "db.write_p99_ns"),
    ("commit-heavy, 1 view, per commit mean", "commit.v1.commit_mean_ns"),
    ("commit-heavy, 1 view, per commit p50", "commit.v1.commit_p50_ns"),
    ("commit-heavy, 1 view, per commit p99", "commit.v1.commit_p99_ns"),
    ("commit-heavy, 2 views, per commit mean", "commit.v2.commit_mean_ns"),
    ("commit-heavy, 2 views, per commit p50", "commit.v2.commit_p50_ns"),
    ("commit-heavy, 2 views, per commit p99", "commit.v2.commit_p99_ns"),
    ("commit-heavy, 2 views, all seen in the other", "commit.v2.seen_all_secs"),
]
print("### Ratio final / base (more than 1 is worse; `*` = beyond noise; medians of 3 runs)\n")
print("| Benchmark | " + " | ".join(STORES) + " |")
print("|---|" + "---|" * len(STORES))
for name, k in ROWS:
    print(f"| {name} | " + " | ".join(cell(s, k) for s in STORES) + " |")

def fmt(v, k):
    if v is None:
        return "n/a"
    med, sp, n = v
    if k.endswith("_ns"):
        return f"{med/1000:.0f} us" if med < 5e6 else f"{med/1e6:.1f} ms"
    if k.endswith("kib"):
        return f"{med/1024:.0f}"
    if med >= 1000:
        return f"{med:,.0f}".replace(",", " ")
    return f"{med:.3g}"

print("\n### Absolute medians (base / final) of the headline numbers\n")
print("| Metric | " + " | ".join(STORES) + " |")
print("|---|" + "---|" * len(STORES))
for name, k in ROWS:
    cells = []
    for s in STORES:
        a, b = stat(s, "base", k), stat(s, "final", k)
        cells.append(f"{fmt(a, k)} / {fmt(b, k)}")
    print(f"| {name} | " + " | ".join(cells) + " |")

print("\n### pc_race with an opener (writes in 20 s, write latency): base against final with W2 and W1 as default, KEEP_CACHE=off with the repair, KEEP_CACHE=off without it\n")
print("| Store | variant | writes | stale read-backs | write p50 | write p99 | repairs | repairs released |")
print("|---|---|---|---|---|---|---|---|")
for s in STORES:
    for ver, var, label in (("base", "opener_default", "base (no W1/W2)"), ("final", "opener_default", "final, defaults"),
                            ("final", "opener_keepoff", "final, KEEP_CACHE=off, repair on"),
                            ("final", "opener_keepoff_repairoff", "final, KEEP_CACHE=off, repair off")):
        d = data.get((s, ver), {})
        g = lambda key: d.get(f"pcrace.{var}.{key}")
        if not g("writes"):
            continue
        def med(key):
            v = g(key)
            return f"{statistics.median(v):,.0f}".replace(",", " ") if v else "-"
        def lat(key):
            v = g(key)
            return (f"{statistics.median(v)/1000:.0f} us" if statistics.median(v) < 5e6 else f"{statistics.median(v)/1e6:.1f} ms") if v else "-"
        print(f"| {s} | {label} | {med('writes')} | {med('stale')} | {lat('write_p50_ns')} | {lat('write_p99_ns')} | {med('kc_repairs')} | {med('kc_repairs_released')} |")
