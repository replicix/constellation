#!/usr/bin/env python3
"""Plan 40 section 8 rerun (P40-01): tables for items 2, 5 and 6 and the big-directory cells.
usage: report-rerun.py OUTROOT   (OUTROOT holds out-write/ out-hd/ out-big1m/ out-big1m-lists/ out-big10m/ out-od/)"""
import glob
import json
import os
import re
import statistics as st
import sys

R = sys.argv[1]
FS = ["ext4", "xfs", "btrfs", "tmpfs", "root"]


def cells(d):
    for f in sorted(glob.glob(os.path.join(R, d, "cell-*.jsonl"))):
        tag = None
        recs = []
        for l in open(f):
            r = json.loads(l)
            recs.append(r)
            tag = r.get("tag", tag)
        m = re.match(r"(\w+?)-(\w+)-r(\d+)$", tag or "")
        if m:
            yield m.group(1), m.group(2), int(m.group(3)), recs


def med(v):
    return st.median(v) if v else float("nan")


def write_tables():
    # (fs, wm, name) -> {version: [walls]}
    t = {}
    for fs, ver, run, recs in cells("out-write"):
        for r in recs:
            if r["kind"] == "run" and (r["name"].startswith("storm_") or r["name"] == "untar"):
                t.setdefault((fs, r["write_mode"], r["name"]), {}).setdefault(ver, []).append((run, r["wall"], r["daemon_cpu"]))
    print("### Item 2: write path (wall seconds; per cell, one value per run)\n")
    for wm in ("through", "back"):
        print(f"**write-{wm}**\n")
        print("| fs | workload | base | off | auto (run 1) | auto (run 2) | auto/off | auto/base |")
        print("|---|---|---|---|---|---|---|---|")
        for fs in FS:
            for name in ("storm_create", "storm_rename", "storm_unlink", "untar"):
                d = t.get((fs, wm, name))
                if not d:
                    continue
                def g(v, run=None):
                    xs = [w for (rn, w, c) in d.get(v, []) if run is None or rn == run]
                    return xs[0] if xs else None
                b, o, a1, a2 = g("base"), g("off"), g("auto", 1), g("auto", 2)
                a = [x for x in (a1, a2) if x]
                am = med(a) if a else None
                f = lambda x: "-" if x is None else f"{x:.1f}"
                print(f"| {fs} | {name} | {f(b)} | {f(o)} | {f(a1)} | {f(a2)} | {'-' if not (am and o) else f'{am/o:.2f}'} | {'-' if not (am and b) else f'{am/b:.2f}'} |")
        print()


def hd_table():
    print("### Item 6: `readdir-huge-delete` (1,000,000-entry directory, `rm -rf` under hint pressure, 1 MiB hint cap)\n")
    print("| fs | created (s) | rm -rf (s) | lister rounds | pinned_dirs max | lost_cookies | cold_scans | result |")
    print("|---|---|---|---|---|---|---|---|")
    for fs in FS:
        p = os.path.join(R, "out-hd", f"hd-{fs}.log")
        if not os.path.exists(p):
            continue
        s = re.sub(r"\x1b\[[0-9;]*m", "", open(p).read())
        m = re.search(r"created (\d+) \+ .* in ([\d.]+)s", s)
        m2 = re.search(r"rm -rf of \d+ entries took ([\d.]+)s; the lister completed (\d+) rounds.*pinned_dirs max (\d+).*lost_cookies (\d+) -> (\d+), cold_scans (\d+) -> (\d+)", s)
        ok = "PASSED" if "readdir-huge-delete PASSED" in s else "FAILED"
        print(f"| {fs} | {float(m.group(2)):.0f} | {float(m2.group(1)):.0f} | {m2.group(2)} | {m2.group(3)} | {m2.group(4)} -> {m2.group(5)} | {m2.group(6)} -> {m2.group(7)} | {ok} |")
    print()


def big_tables():
    print("### 1M and 10M directories (median of the runs; `ls -l` and `ls -f` kernel-cold unless noted)\n")
    print("| cell | version | populate (s) | ls -f (s) | ls -f cpu (s) | ls -f RSS (MiB) | ls -l (s) | ls -l cpu (s) | ls -l RSS (MiB) | cold resume (ms) | cold-scan ref ls -f (s) |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for d in ("out-big1m", "out-big1m-lists", "out-big10m"):
        for fs, ver, run, recs in cells(d):
            size = 10_000_000 if d == "out-big10m" else 1_000_000
            runs = {}
            res = {}
            for r in recs:
                if r["kind"] == "run":
                    runs.setdefault(r["name"], []).append(r)
                elif r["kind"] == "result":
                    res[r["name"]] = r
            if not runs:
                continue
            m = lambda n, k, div=1: (med([x[k] for x in runs[n]]) / div) if n in runs else float("nan")
            cr = res.get("cold_resume", {}).get("line", "")
            mm = re.search(r"first_entry_ms=([\d.]+)", cr)
            print(f"| {fs} {size//1000000}M | {ver} | {res.get('populate', {}).get('wall', float('nan')):.0f} | {m('big_ls_f','wall'):.2f} | {m('big_ls_f','daemon_cpu'):.1f} | {m('big_ls_f','rss_peak_kb',1024):.0f} | {m('big_ls_l','wall'):.1f} | {m('big_ls_l','daemon_cpu'):.1f} | {m('big_ls_l','rss_peak_kb',1024):.0f} | {mm.group(1) if mm else '-'} | {m('big_ls_f_cold_scan_ref','wall'):.2f} |")
    print()
    print("### Pins, `releasedir`, handover (one stream pinned at the middle of the directory, `/dev/fuse` session)\n")
    print("| cell | pinned bytes | per listed name (B) | RSS with the pin (MiB) | closedir p50 / p99 / max (us) | handover wall (s) | largest memfd (MiB) |")
    print("|---|---|---|---|---|---|---|")
    for d in ("out-big1m", "out-big10m"):
        for fs, ver, run, recs in cells(d):
            res = {r["name"]: r for r in recs if r["kind"] == "result"}
            if "big_pinned" not in res:
                continue
            bp, rl, ho = res["big_pinned"], res.get("releasedir", {}), res.get("handover_pinned", {})
            mm = re.search(r"p50=(\d+) p99=(\d+) max=(\d+)", rl.get("line", ""))
            n = bp["size"]
            print(f"| {fs} {n//1000000}M | {bp['readdir'].get('pinned_bytes')} | {bp['pinned_bytes_per_name']} | {bp['rss_kb']/1024:.0f} | {'/'.join(mm.groups()) if mm else '-'} | {ho.get('wall')} (rc {ho.get('rc')}) | {ho.get('max_memfd', 0)/2**20:.0f} |")
    print()


def od_table():
    print("### Item 5: daemon RSS with open directories (10,000 directories of 100 names, one stream each, one name read)\n")
    print("| fs | version | RSS idle (MiB) | +1,000 open | +10,000 open | delta at 10,000 (MiB) | kernel_refs at 10,000 | pinned bytes | pinned dirs | releasedir p50 / p99 / max of 1,000 (us) |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for fs in FS:
        for ver in ("base", "auto"):
            for f, v, run, recs in cells("out-od"):
                if f != fs or v != ver:
                    continue
                res = [r for r in recs if r["kind"] == "result"]
                b = next(r for r in res if r["name"] == "opendirs_base")["rss_kb"] / 1024
                o = [r for r in res if r["name"] == "opendirs_open"]
                o1 = next(r for r in o if r["size"] == 1000)
                o10 = [r for r in o if r["size"] == 10000][-1]
                rl = next(r for r in res if r["name"] == "opendirs_release")["line"]
                mm = re.search(r"p50=(\d+) p99=(\d+) max=(\d+)", rl)
                rd = o10.get("readdir") or {}
                print(f"| {fs} | {ver} | {b:.0f} | {o1['rss_kb']/1024:.0f} | {o10['rss_kb']/1024:.0f} | {o10['rss_kb']/1024 - b:.0f} | {o10.get('kernel_refs')} | {rd.get('pinned_bytes', '-')} | {rd.get('pinned_dirs', '-')} | {'/'.join(mm.groups())} |")
    print()


for fn in (write_tables, hd_table, big_tables, od_table):
    fn()
