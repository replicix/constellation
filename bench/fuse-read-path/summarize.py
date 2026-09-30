#!/usr/bin/env python3
"""Markdown tables from run.sh JSONL results: median of the repeats per cell.

usage: summarize.py RESULTS.jsonl [...]      one table per (workload, cache)
       summarize.py --line < one.jsonl        one-line summary per record
"""
import json
import statistics
import sys
from collections import defaultdict

MODE_ORDER = ["copy", "memcache", "mmap", "splice", "splice-nomove", "vmsplice", "vmsplice-gift",
              "uring", "uring-bufpool", "uring-zc", "passthrough"]
WL_ORDER = ["seq1", "seq1-dio", "seq8", "rand4k-1j", "rand4k-8j", "rand4k-1j-dio", "rand4k-8j-dio",
            "rand128k-aio", "smallfiles"]


def get(r, path):
    for k in path.split("."):
        if not isinstance(r, dict) or k not in r:
            return None
        r = r[k]
    return r


def med(rows, path):
    v = [get(r, path) for r in rows]
    v = [x for x in v if isinstance(x, (int, float))]
    return statistics.median(v) if v else None


def fmt(x, nd=1):
    if x is None:
        return "-"
    if isinstance(x, float) and abs(x) >= 1000:
        return f"{x:,.0f}"
    return f"{x:.{nd}f}" if isinstance(x, float) else str(x)


def line(r):
    f = r.get("fio") or {}
    d = r.get("daemon") or {}
    s = (f"{r.get('workload')}/{r.get('cache')}/{r.get('mode')} r{r.get('rep')}: {r.get('status')}"
         f"  {fmt(f.get('bw_mib_s'))} MiB/s  {fmt(f.get('iops'), 0)} IOPS"
         f"  lat {fmt(f.get('lat_mean_us'))}us p99 {fmt(f.get('clat_p99_us'))}us"
         f"  daemon {fmt(d.get('cpu_s_per_gib'), 3)} cpu-s/GiB rss {fmt((d.get('maxrss_kib') or 0) / 1024, 0)} MiB"
         f"  sys {fmt(r.get('sys_cpu_busy_pct'))}%  disk {fmt(r.get('disk_read_mib'))} MiB"
         f"  req p50 {d.get('req_size_p50')} max {d.get('req_size_max')}")
    if r.get("error"):
        s += f"  ERROR: {r['error']}"
    for c in r.get("checks") or []:
        s += f"  [!] {c}"
    return s


def order(v, lst):
    return (lst.index(v) if v in lst else len(lst), v)


def table(rows, out):
    cells = defaultdict(list)
    for r in rows:
        cells[(r.get("workload"), r.get("cache"), r.get("mode"))].append(r)
    groups = sorted({(w, c) for w, c, _ in cells}, key=lambda k: (order(k[0], WL_ORDER), k[1] != "warm"))
    first = rows[0]
    out.write(f"# FUSE read path: {first.get('host')} kernel {first.get('kernel')}, "
              f"libfuse {str(first.get('libfuse_commit'))[:12]}, {first.get('fio_version')}, "
              f"store on {first.get('store_fs')}, chunk {first.get('chunk_size')}\n\n")
    out.write("Medians over repeats. `vs copy` = throughput relative to the copy baseline of the same "
              "workload/cache. `cpu-s/GiB` = daemon user+sys CPU seconds per GiB it served "
              "(passthrough serves nothing: see `sys %`).\n")
    for wl, cache in groups:
        modes = sorted({m for w, c, m in cells if w == wl and c == cache}, key=lambda m: order(m, MODE_ORDER))
        base = med(cells.get((wl, cache, "copy"), []), "fio.bw_mib_s")
        out.write(f"\n## {wl} / {cache}\n\n")
        out.write("| mode | n | MiB/s | vs copy | IOPS | mean lat us | p99 us | daemon cpu-s/GiB "
                  "| daemon usr/sys s | daemon RSS MiB | sys CPU % | disk MiB | req p50/max | notes |\n")
        out.write("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
        for m in modes:
            rs = cells[(wl, cache, m)]
            ok = [r for r in rs if r.get("status") == "ok"]
            bad = [r for r in rs if r.get("status") != "ok"]
            notes = sorted({c for r in rs for c in (r.get("checks") or [])})
            notes += sorted({f"{r.get('status')}: {r.get('error')}" for r in bad})
            bw = med(ok, "fio.bw_mib_s")
            rel = f"{bw / base:.2f}x" if bw and base else "-"
            p50 = sorted({str(get(r, "daemon.req_size_p50")) for r in ok})
            mx = max([get(r, "daemon.req_size_max") or 0 for r in ok], default=0)
            rss = med(ok, "daemon.maxrss_kib")
            out.write(f"| {m} | {len(ok)}{'+' + str(len(bad)) + ' failed' if bad else ''} | {fmt(bw)} | {rel} "
                      f"| {fmt(med(ok, 'fio.iops'), 0)} | {fmt(med(ok, 'fio.lat_mean_us'))} "
                      f"| {fmt(med(ok, 'fio.clat_p99_us'))} | {fmt(med(ok, 'daemon.cpu_s_per_gib'), 3)} "
                      f"| {fmt(med(ok, 'daemon.utime_s'), 2)}/{fmt(med(ok, 'daemon.stime_s'), 2)} "
                      f"| {fmt(rss / 1024 if rss else None, 0)} | {fmt(med(ok, 'sys_cpu_busy_pct'))} "
                      f"| {fmt(med(ok, 'disk_read_mib'), 0)} | {'/'.join(p50)}/{fmt(mx // 1024 if mx else None)}K "
                      f"| {'; '.join(notes)} |\n")


def main():
    args = sys.argv[1:]
    if args and args[0] == "--line":
        for l in sys.stdin:
            if l.strip():
                print(line(json.loads(l)))
        return
    if not args:
        sys.exit(__doc__)
    rows = []
    for p in args:
        with open(p) as f:
            rows += [json.loads(l) for l in f if l.strip()]
    if not rows:
        sys.exit("no results")
    table(rows, sys.stdout)


if __name__ == "__main__":
    main()
