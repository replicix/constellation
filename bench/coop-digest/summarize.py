#!/usr/bin/env python3
"""Median-and-spread tables from the interleaved timing runs (t-lookup,
t-steady, t-repair). Usage: summarize.py <raw dir> [lookup|steady|repair]"""
import re
import statistics
import sys
from collections import defaultdict


def nums(cell):
    return [float(x) for x in re.findall(r"[-+]?\d+(?:\.\d+)?(?:e[-+]?\d+)?", cell)]


def med(v):
    return statistics.median(v)


def fmt(v, digits=0):
    if not v:
        return "n/a"
    m = med(v)
    return f"{m:.{digits}f} ({min(v):.{digits}f}..{max(v):.{digits}f})"


def rows(path):
    out = []
    for line in open(path):
        line = line.rstrip("\n")
        if line.startswith("|") and not line.startswith("| rep |") and not line.startswith("| candidate") and not line.startswith("| scenario"):
            out.append([c.strip() for c in line.strip().strip("|").split("|")])
        elif line.startswith("## "):
            out.append(["##", line[3:]])
    return out


def lookup(d):
    g = defaultdict(lambda: defaultdict(list))
    for r in rows(f"{d}/t-lookup.md"):
        if r[0] == "##" or len(r) < 10:
            continue
        cand, n, p = r[1], int(r[2]), int(r[3])
        hit, miss, single = nums(r[4]), nums(r[5]), nums(r[6])
        c = g[(n, p)][cand]
        c.append((hit[0], hit[1], miss[0], miss[1], single[0], single[1]))
    for (n, p), cs in sorted(g.items()):
        print(f"\n#### n = {n:,} keys per peer, {p} peers (fan-out over all peers; ns; median (min..max) of {max(len(v) for v in cs.values())} runs)\n")
        print("| candidate | hit p50 | hit p99 | miss p50 | miss p99 | single-peer hit mean | single-peer miss mean | miss p50 vs C1 |")
        print("|---|---|---|---|---|---|---|---|")
        base = None
        for cand, v in cs.items():
            if cand.startswith("C1"):
                base = med([x[2] for x in v])
        for cand, v in cs.items():
            cols = [[x[i] for x in v] for i in range(6)]
            ratio = f"{med(cols[2]) / base:.2f}x" if base else ""
            print(f"| {cand} | " + " | ".join(fmt(c) if i < 4 else fmt(c, 1) for i, c in enumerate(cols)) + f" | {ratio} |")


def steady(d):
    g = defaultdict(lambda: defaultdict(list))
    for r in rows(f"{d}/t-steady.md"):
        if r[0] == "##" or len(r) < 12:
            continue
        cand, n, ch = r[0], int(r[1]), r[2]
        pub, rcv = nums(r[4]), nums(r[5])
        g[(n, ch)][cand].append((pub[0], pub[1], pub[2], rcv[0], rcv[1], rcv[2]))
    for (n, ch), cs in sorted(g.items(), key=lambda x: (x[0][0], float(x[0][1].strip('%')))):
        print(f"\n#### n = {n:,}, churn {ch} per minute (per 250 ms tick, microseconds; median (min..max) over runs)\n")
        print("| candidate | publisher absorb mean | publisher p99 | receiver apply mean | receiver p99 | receiver mean vs C1 |")
        print("|---|---|---|---|---|---|")
        base = None
        for cand, v in cs.items():
            if cand.startswith("C1"):
                base = med([x[5] for x in v])
        for cand, v in cs.items():
            cols = [[x[i] / 1000 for x in v] for i in (2, 1, 5, 4)]
            ratio = f"{med([x[5] for x in v]) / base:.2f}x" if base else ""
            print(f"| {cand} | " + " | ".join(fmt(c, 1) for c in cols) + f" | {ratio} |")


def repair(d):
    g = defaultdict(lambda: defaultdict(list))
    ctx = ""
    for r in rows(f"{d}/t-repair.md"):
        if r[0] == "##":
            m = re.search(r"n=(\d+)", r[1])
            ctx = int(m.group(1)) if m else ctx
            continue
        if len(r) < 10 or r[3] == "n/a":
            continue
        sc, cand, method, result = r[0], r[1], r[2], r[3]
        owner, recv = nums(r[7])
        g[(ctx, sc)][(cand, method)].append((owner, recv, result, r[6], r[5]))
    for (n, sc), cs in sorted(g.items()):
        print(f"\n#### n = {n:,}, scenario {sc} (CPU ms, median (min..max) over runs)\n")
        print("| candidate | method | result | rounds | wire | owner CPU ms | receiver CPU ms |")
        print("|---|---|---|---|---|---|---|")
        for (cand, method), v in cs.items():
            res = "ok" if all(x[2] == "ok" for x in v) else "FAILED"
            print(f"| {cand} | {method} | {res} | {v[0][4]} | {v[0][3]} | {fmt([x[0] for x in v], 1)} | {fmt([x[1] for x in v], 1)} |")


if __name__ == "__main__":
    d = sys.argv[1]
    which = sys.argv[2] if len(sys.argv) > 2 else "all"
    if which in ("lookup", "all"):
        lookup(d)
    if which in ("steady", "all"):
        steady(d)
    if which in ("repair", "all"):
        repair(d)
