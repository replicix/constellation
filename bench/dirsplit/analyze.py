#!/usr/bin/env python3
"""Summarize dirsplit-bench JSON lines (one per phase) into Markdown tables.

    analyze.py OUT_DIR_OR_FILES... > tables.md

Per (cell, workload, configuration): the median over the repetitions,
and the spread (min-max) of total throughput; op latency p50/p90/p99 (the
median of each repetition's percentile); per-node throughput (median of
the repetitions' slowest and fastest writer); root CPU; S3 PUT and GET
per second over every node; errors; how often the placement delegated the
directory whole (a); whether every phase converged. Then the d/a ratio
per cell and workload with its spread over the repetitions (each
repetition's d over the same repetition's a). Phases of the deleted
configurations b and c and of the `own` workload (old result files) are
skipped.
"""
import json
import os
import re
import statistics
import sys
from collections import defaultdict

CONFIG_NAMES = {
    "a": "a placement",
    "d": "d whole to w0",
}
WORKLOADS = ["uniform", "mix", "rename"]


def load(paths):
    rows, failed = [], []
    for p in paths:
        files = [os.path.join(p, f) for f in sorted(os.listdir(p)) if f.endswith(".jsonl")] if os.path.isdir(p) else [p]
        for f in files:
            with open(f) as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    r = json.loads(line)
                    (failed if "failed" in r else rows).append(r)
    return rows, failed


def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else float("nan")


def fmt(x, nd=0):
    if x is None or x != x:
        return "-"
    if nd == 0:
        return f"{x:.0f}"
    return f"{x:.{nd}f}"


def lat(x):
    if x is None or x != x:
        return "-"
    return f"{x:.1f}" if x < 100 else f"{x:.0f}"


def cell_order(label):
    # T1..T5 by writers, then the sweep by S3 latency and root RTT.
    m = re.match(r"X-rtt([\d.]+)-s3(\d+)-w(\d+)$", label)
    if m:
        return (1, float(m.group(2)), float(m.group(1)), label)
    t, w = label.rsplit("-w", 1)
    return (0, int(w), t, label)


def summarize(rows):
    groups = defaultdict(list)
    for r in rows:
        groups[(r["label"], r["workload"], r["config"])].append(r)
    out = []
    for (label, wl, cfg), rs in groups.items():
        rs.sort(key=lambda r: r["rep"])
        thr = [r["ops_per_s"] for r in rs]
        per_node_min = [min(r["per_node_ops_per_s"]) for r in rs]
        per_node_max = [max(r["per_node_ops_per_s"]) for r in rs]
        s3put = [sum(s["put"] for s in r["s3"]) / r["measure_s"] for r in rs]
        s3get = [sum(s["get"] + s["list"] + s["head"] for s in r["s3"]) / r["measure_s"] for r in rs]
        out.append({
            "label": label, "workload": wl, "config": cfg, "n": len(rs),
            "reps": [r["rep"] for r in rs],
            "ops": med(thr), "ops_min": min(thr), "ops_max": max(thr),
            "node_min": med(per_node_min), "node_max": med(per_node_max),
            "p50": med([r["latency"]["p50_ms"] for r in rs]),
            "p90": med([r["latency"]["p90_ms"] for r in rs]),
            "p99": med([r["latency"]["p99_ms"] for r in rs]),
            "root_cpu": med([r["root_cpu_pct"] for r in rs]),
            "s3put": med(s3put), "s3get": med(s3get),
            "errors": sum(r["errors_total"] for r in rs),
            "split_fired": sum(1 for r in rs if r.get("split_fired")),
            "placed_whole": sum(1 for r in rs if r.get("placed_whole")),
            "converged": all(c["ok"] for r in rs for c in r["converged"]),
            "by_rep": {r["rep"]: r["ops_per_s"] for r in rs},
            "p50_by_rep": {r["rep"]: r["latency"]["p50_ms"] for r in rs},
        })
    return out


def main():
    rows, failed = load(sys.argv[1:])
    summ = summarize(rows)
    by = {(s["label"], s["workload"], s["config"]): s for s in summ}
    labels = sorted({s["label"] for s in summ}, key=cell_order)
    print("## Per cell\n")
    print("Throughput is ops/s over all writers (median of the repetitions, "
          "min-max in brackets); node lo/hi the slowest and fastest writer's ops/s; "
          "latencies in ms; S3 requests per second over every node; "
          "`whole` how many "
          "a phases ran with the placement's whole-directory delegation in place.\n")
    for label in labels:
        print(f"### {label}\n")
        print("| workload | config | n | ops/s [min-max] | node lo/hi | p50 | p90 | p99 | root CPU % | S3 PUT/s | S3 GET/s | errors | whole | converged |")
        print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
        for wl in WORKLOADS:
            for cfg in "ad":
                s = by.get((label, wl, cfg))
                if not s:
                    continue
                print(f"| {wl} | {CONFIG_NAMES[cfg]} | {s['n']} | "
                      f"{fmt(s['ops'])} [{fmt(s['ops_min'])}-{fmt(s['ops_max'])}] | "
                      f"{fmt(s['node_min'])}/{fmt(s['node_max'])} | {lat(s['p50'])} | {lat(s['p90'])} | {lat(s['p99'])} | "
                      f"{fmt(s['root_cpu'])} | {fmt(s['s3put'], 1)} | {fmt(s['s3get'], 1)} | {s['errors']} | "
                      f"{s['placed_whole'] if cfg == 'a' else '-'} | "
                      f"{'yes' if s['converged'] else 'NO'} |")
        print()
    print("## d against a\n")
    print("Each repetition's whole-delegation throughput over the same repetition's "
          "placement throughput: median [min-max]; above 1 the delegation is faster. p50 "
          "ratio likewise (below 1 it is faster).\n")
    print("| cell | workload | d/a ops/s | d/a p50 |")
    print("|---|---|---|---|")
    for label in labels:
        for wl in WORKLOADS:
            a = by.get((label, wl, "a"))
            if not a:
                continue

            def ratio(cfg, key="by_rep"):
                o = by.get((label, wl, cfg))
                if not o:
                    return "-"
                rs = [o[key][k] / a[key][k] for k in o[key] if k in a[key] and a[key][k]]
                if not rs:
                    return "-"
                return f"{statistics.median(rs):.2f} [{min(rs):.2f}-{max(rs):.2f}]"
            print(f"| {label} | {wl} | {ratio('d')} | {ratio('d', 'p50_by_rep')} |")
    if failed:
        print("\n## Failed phases\n")
        for r in failed:
            print(f"- {r['label']} rep {r['rep']} {r['workload']}/{r['config']}: {r['failed'][:300]}")


if __name__ == "__main__":
    main()
