#!/usr/bin/env python3
"""Plan 40 section 8: markdown tables from cells of abba.sh (pair cells: off and auto alternated in one populated
state dir; base cells: the build before plan 40's listing commits).

  report.py OUTDIR [OUTDIR ...]     the out dirs of abba.sh (list, trees, big, base)

Prints, per section: wall-time medians (ms) per file system with the auto/off ratio; FUSE requests per run (and
without the per-name `listxattr` that `ls -l` sends, which no READDIRPLUS page can answer); the tree workloads cold and
warm; the daemon's service time per directory page (`readdir` + `readdirplus`, from the ops' summed service time).
"""
import glob
import json
import os
import statistics
import sys
from collections import defaultdict

STORES = ["ext4", "xfs", "btrfs", "tmpfs"]
LIST_ORDER = ["ls_f", "ls", "find_name", "ls_l", "ls_la", "find_ls", "find_newer", "find_size", "du_s", "rsync_n",
              "ls_l_2nd", "xargs_stat"]
TREE_ORDER = ["ls_lR", "find_f", "git_status", "make_q", "rsync_n"]


def load(dirs):
    runs = defaultdict(list)      # (store, version, name, size) -> runs
    results = []
    for d in dirs:
        for f in sorted(glob.glob(os.path.join(d, "cell-*.jsonl"))):
            store, ver = os.path.basename(f).split("-")[1:3]
            for line in open(f):
                r = json.loads(line)
                v = r.get("mode") if r.get("mode") in ("off", "auto", "always") else ver
                if r["kind"] == "run":
                    runs[(store, v, r["name"], r.get("size"))].append(r)
                elif r["kind"] == "result":
                    results.append((store, v, r))
    return runs, results


def med(rs, key):
    v = [r[key] for r in rs if r.get(key) is not None]
    return statistics.median(v) if v else None


def q(v, p):
    v = sorted(v)
    k = (len(v) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (k - lo)


def iqr(rs, key):
    v = [r[key] for r in rs if r.get(key) is not None]
    return q(v, .75) - q(v, .25) if len(v) > 1 else 0.0


def ms(x):
    return "" if x is None else (f"{x * 1000:.1f}" if x * 1000 >= 10 else f"{x * 1000:.2f}")


def nox(rs):
    """median request count without the per-name listxattr"""
    v = [r["requests"] - r["reqs"].get("listxattr", 0) for r in rs]
    return statistics.median(v) if v else None


def pages(rs):
    """(daemon service ns per directory page, pages) over the runs: readdir + readdirplus"""
    ns = sum(r.get("ns_by_op", {}).get("readdir", 0) + r.get("ns_by_op", {}).get("readdirplus", 0) for r in rs)
    n = sum(r["reqs"].get("readdir", 0) + r["reqs"].get("readdirplus", 0) for r in rs)
    return (ns / n, n / len(rs)) if n else (None, 0)


def ratio(a, b):
    return "" if not a or not b else f"{a / b:.2f}"


def main(dirs):
    runs, results = load(dirs)
    stores = [s for s in STORES if any(k[0] == s for k in runs)]
    sizes = sorted({k[3] for k in runs if k[2] in LIST_ORDER})

    have_any_base = any(k[1] == "base" and k[2] in LIST_ORDER for k in runs)
    print("## Ratios of kernel-cold wall time (median), per file system: auto/off" +
          (", auto/base, off/base" if have_any_base else "") + "\n")
    cols = [("auto/off", "auto", "off")] + ([("auto/base", "auto", "base"), ("off/base", "off", "base")] if have_any_base else [])
    print("| workload | entries | " + " | ".join(f"{c[0]} {s}" for c in cols for s in stores) + " |")
    print("|---|---|" + "---|" * (len(cols) * len(stores)))
    for name in LIST_ORDER:
        for size in sizes:
            row = []
            for _, num, den in cols:
                for s in stores:
                    x, y = runs.get((s, num, name, size)), runs.get((s, den, name, size))
                    row.append(ratio(med(x, "wall"), med(y, "wall")) if x and y else "")
            if any(row):
                print(f"| {name} | {size} | " + " | ".join(row) + " |")
    print()

    print("## Listing workloads: kernel-cold wall time, median ms (IQR); ratio = auto / off\n")
    for store in stores:
        print(f"#### {store}\n")
        have_base = any(k[0] == store and k[1] == "base" and k[2] in LIST_ORDER for k in runs)
        print("| workload | entries | " + ("base | " if have_base else "") + "off | auto | auto/off |" +
              (" auto/base |" if have_base else ""))
        print("|---|---|" + ("---|" if have_base else "") + "---|---|---|" + ("---|" if have_base else ""))
        for name in LIST_ORDER:
            for size in sizes:
                o = runs.get((store, "off", name, size))
                a = runs.get((store, "auto", name, size))
                if not o or not a:
                    continue
                b = runs.get((store, "base", name, size)) if have_base else None
                cells = []
                for rs in ([b] if have_base else []) + [o, a]:
                    cells.append(f"{ms(med(rs, 'wall'))} ({ms(iqr(rs, 'wall'))})" if rs else "")
                print(f"| {name} | {size} | " + " | ".join(cells) + f" | {ratio(med(a, 'wall'), med(o, 'wall'))} |" +
                      (f" {ratio(med(a, 'wall'), med(b, 'wall'))} |" if have_base else ""))
        print()

    print("## FUSE requests per run (median); `x` = without the per-name `listxattr`\n")
    req_rows = defaultdict(dict)
    for store in stores:
        have_base = any(k[0] == store and k[1] == "base" and k[2] in LIST_ORDER for k in runs)
        for name in ("ls_l", "ls_la", "find_ls", "du_s", "ls_l_2nd", "rsync_n"):
            for size in sizes:
                for v in ("base", "off", "auto"):
                    rs = runs.get((store, v, name, size))
                    if rs:
                        req_rows[(name, size, v)][store] = (med(rs, "requests"), nox(rs))
    first = stores[0]
    spread = max([max(x[0] for x in d.values()) - min(x[0] for x in d.values()) for d in req_rows.values()] or [0])
    print(f"#### {first} (the other file systems differ by at most {spread:.0f} requests in any cell)\n")
    print("| workload | entries | base / base x | off / off x | auto / auto x | auto vs base: fewer, without `listxattr` (with) |")
    print("|---|---|---|---|---|---|")
    for name in ("ls_l", "ls_la", "find_ls", "du_s", "ls_l_2nd", "rsync_n"):
        for size in sizes:
            cells, vals = [], {}
            for v in ("base", "off", "auto"):
                x = req_rows.get((name, size, v), {}).get(first)
                vals[v] = x
                cells.append(f"{x[0]:.0f} / {x[1]:.0f}" if x else "")
            if not vals["auto"]:
                continue
            red = ""
            if name in ("ls_l", "ls_la") and vals["base"]:
                red = (f"{(1 - vals['auto'][1] / vals['base'][1]) * 100:.1f} % "
                       f"({(1 - vals['auto'][0] / vals['base'][0]) * 100:.1f} %)")
            print(f"| {name} | {size} | " + " | ".join(cells) + f" | {red} |")
    print()

    print("## Daemon service time per directory page (`readdir` + `readdirplus`), us, and pages per run\n")
    print("| workload | entries | " + " | ".join(f"{s} off | {s} auto" for s in stores) + " |")
    print("|---|---|" + "---|---|" * len(stores))
    names = ["ls_f", "ls", "ls_l"]
    for name in names:
        for size in sorted({k[3] for k in runs if k[2] in ("ls_f", "ls", "ls_l", "big_ls_f", "big_ls_l")}):
            nm = name if size <= 100000 else "big_" + name
            row = []
            for s in stores:
                for v in ("off", "auto"):
                    rs = runs.get((s, v, nm, size))
                    p = pages(rs) if rs else (None, 0)
                    row.append(f"{p[0] / 1000:.2f} ({p[1]:.0f} pages)" if p[0] else "")
            if any(row):
                print(f"| {nm} | {size} | " + " | ".join(row) + " |")
    print()

    print("## Daemon CPU (user + sys seconds per run, median) for `ls -l`, and peak RSS (MiB) at 1M entries\n")
    print("| entries | " + " | ".join(f"{st} {v}" for st in stores for v in ("base", "off", "auto")) + " |")
    print("|---|" + "---|" * (3 * len(stores)))
    for size in sizes + sorted({k[3] for k in runs if k[2] == "big_ls_l"}):
        nm = "ls_l" if size <= 100000 else "big_ls_l"
        row = []
        for st in stores:
            for v in ("base", "off", "auto"):
                rs = runs.get((st, v, nm, size))
                if not rs:
                    row.append("")
                elif size > 100000:
                    row.append(f"{med(rs, 'daemon_cpu'):.1f} s, {med(rs, 'rss_peak_kb') / 1024:.0f} MiB")
                else:
                    row.append(f"{med(rs, 'daemon_cpu'):.2f}")
        if any(row):
            print(f"| {size} | " + " | ".join(row) + " |")
    print()

    tsizes = 50000
    if any(k[2].startswith("tree_") for k in runs):
        print("## Tree workloads (50k files, 2,000 directories), wall ms median (IQR); ratio = auto / off\n")
        print("| workload | state | " + " | ".join(f"{s} off | {s} auto | {s} ratio" for s in stores) + " |")
        print("|---|---|" + "---|---|---|" * len(stores))
        for w in TREE_ORDER:
            for state in ("cold", "warm"):
                row = []
                for s in stores:
                    o = runs.get((s, "off", f"tree_{w}_{state}", tsizes))
                    a = runs.get((s, "auto", f"tree_{w}_{state}", tsizes))
                    if not o or not a:
                        row += ["", "", ""]
                        continue
                    row += [f"{ms(med(o, 'wall'))} ({ms(iqr(o, 'wall'))})", f"{ms(med(a, 'wall'))} ({ms(iqr(a, 'wall'))})",
                            ratio(med(a, 'wall'), med(o, 'wall'))]
                print(f"| {w} | {state} | " + " | ".join(row) + " |")
        print("\nRequests per run, off / auto (median), and daemon service ms:\n")
        print("| workload | state | " + " | ".join(f"{s} reqs off/auto | {s} svc ms off/auto" for s in stores) + " |")
        print("|---|---|" + "---|---|" * len(stores))
        for w in TREE_ORDER:
            for state in ("cold", "warm"):
                row = []
                for s in stores:
                    o = runs.get((s, "off", f"tree_{w}_{state}", tsizes))
                    a = runs.get((s, "auto", f"tree_{w}_{state}", tsizes))
                    if not o or not a:
                        row += ["", ""]
                        continue
                    svc = lambda rs: statistics.median(sum(r.get("ns_by_op", {}).values()) / 1e6 for r in rs)
                    row += [f"{med(o, 'requests'):.0f} / {med(a, 'requests'):.0f}", f"{svc(o):.0f} / {svc(a):.0f}"]
                print(f"| {w} | {state} | " + " | ".join(row) + " |")
        print()

    big = [(s, v, r) for s, v, r in results if r["name"] in ("cold_resume", "populate") and r.get("size", 0) >= 1000000]
    if big:
        print("## Big directories (results)\n")
        for s, v, r in big:
            print(f"- {s} {v} {r['name']} {r.get('size')}: " +
                  json.dumps({k: x for k, x in r.items() if k not in ("kind", "tag", "t", "name", "size", "mode")}))
        print()

    n = {s: {v: len(runs.get((s, v, "ls_l", sizes[0] if sizes else 0), [])) for v in ("base", "off", "auto")} for s in stores}
    print("Runs per cell group (`ls_l` at the smallest size):", json.dumps(n))


main(sys.argv[1:])
