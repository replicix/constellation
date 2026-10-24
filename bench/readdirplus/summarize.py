#!/usr/bin/env python3
"""Summarise abba.sh's cells against plan 40 section 8's acceptance.
  summarize.py OUTDIR       prints one table per store: median wall time (IQR), requests, daemon CPU and peak RSS per
                            workload and size for each version, the verdict of acceptance items 1-4 and 7 per cell
                            group (R0 = base), and the section 9 measure-later items (auto against always, the strict
                            and handoff numbers when the big phase ran).
"""
import glob
import json
import os
import statistics
import sys
from collections import defaultdict


def q(v, p):
    v = sorted(v)
    k = (len(v) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (k - lo)


def load(outdir):
    runs = defaultdict(list)   # (store, version, name, size) -> [run dicts]
    results = []
    for f in sorted(glob.glob(os.path.join(outdir, "cell-*.jsonl"))):
        store, ver = os.path.basename(f).split("-")[1:3]
        for line in open(f):
            r = json.loads(line)
            if r["kind"] == "run":
                runs[(store, ver, r["name"], r.get("size"))].append(r)
            elif r["kind"] == "result":
                results.append((store, ver, r))
    return runs, results


def stat(rs, key):
    v = [r[key] for r in rs if r.get(key) is not None]
    return (statistics.median(v), q(v, .75) - q(v, .25)) if v else (None, None)


def main(outdir):
    runs, results = load(outdir)
    stores = sorted({k[0] for k in runs})
    for store in stores:
        print(f"\n## {store}\n")
        names = sorted({(k[2], k[3]) for k in runs if k[0] == store}, key=lambda x: (str(x[1]), x[0]))
        versions = [v for v in ("base", "off", "auto", "always") if any(k[0] == store and k[1] == v for k in runs)]
        print("| workload | size | " + " | ".join(f"{v} wall s (IQR) / reqs / cpu s" for v in versions) + " |")
        print("|---|---|" + "---|" * len(versions))
        for name, size in names:
            cells = []
            for v in versions:
                rs = runs.get((store, v, name, size), [])
                if not rs:
                    cells.append("")
                    continue
                w, iqr = stat(rs, "wall")
                rq, _ = stat(rs, "requests")
                cpu, _ = stat(rs, "daemon_cpu")
                cells.append(f"{w:.3f} ({iqr:.3f}) / {rq:.0f} / {cpu:.2f}" if cpu is not None else f"{w:.3f} ({iqr:.3f}) / {rq:.0f}")
            print(f"| {name} | {size} | " + " | ".join(cells) + " |")
        print("\nAcceptance (against base):")
        for name, size in names:
            b = runs.get((store, "base", name, size))
            if not b:
                continue
            bw, biqr = stat(b, "wall")
            br, _ = stat(b, "requests")
            for v in versions[1:]:
                rs = runs.get((store, v, name, size))
                if not rs:
                    continue
                w, iqr = stat(rs, "wall")
                r, _ = stat(rs, "requests")
                note = ""
                if name in ("ls_f", "ls", "find_name"):
                    band = max(0.03 * bw, biqr)
                    ok = (w <= bw + band) if size <= 1000 else (w < bw)
                    note = f"item 1 {'ok' if ok else 'FAIL'} (wall {w / bw:.2f}x)"
                elif name in ("ls_l", "ls_la") and size in (1000, 10000):
                    red = 1 - r / br if br else 0
                    note = f"item 3 {'ok' if red >= .9 else 'FAIL'} (requests -{red * 100:.0f}%)"
                elif name in ("ls_l", "find_ls", "du_s", "ls_l_2nd", "rsync_n"):
                    note = f"item 4 {'ok' if w < bw else 'FAIL'} (wall {w / bw:.2f}x)"
                elif name.startswith("storm_") or name == "untar":
                    band = max(0.03 * bw, biqr)
                    note = f"item 2 {'ok' if abs(w - bw) <= band else 'FAIL'} (wall {w / bw:.2f}x)"
                if note:
                    print(f"- {store} {v} {name} {size}: {note}")
        print("\nSection 9 measure-later items:")
        for name, size in names:
            a, al = runs.get((store, "auto", name, size)), runs.get((store, "always", name, size))
            if a and al and name in ("ls_f", "ls", "find_name", "ls_l"):
                print(f"- always / auto, {name} {size}: wall {stat(al, 'wall')[0] / stat(a, 'wall')[0]:.2f}x, "
                      f"requests {stat(al, 'requests')[0]:.0f} vs {stat(a, 'requests')[0]:.0f}")
        for st, ver, r in results:
            if st == store and r["name"] not in ("populate",):
                print(f"- {ver} {r['name']} {r.get('size')}: " + json.dumps({k: v for k, v in r.items() if k not in ('kind', 'tag', 't', 'name', 'size')}))


main(sys.argv[1])
