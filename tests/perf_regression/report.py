#!/usr/bin/env python3
"""Render a perf-regression suite result (run_suite.py --out) as Markdown.

    report.py HEAD.json [--base BASE.json] [--meta TEXT]

With --base, every metric is also compared with the same metric in the base
result (CI: the latest successful run on main). A base measured with a
different corpus (seed, limit, file-size cap) is not comparable and is ignored.
"""

import argparse
import json
import math
import sys
from pathlib import Path

MARKER = "<!-- perf-regression-report -->"
PROFILES = ["full", "latency250", "bw50mbps"]
PROFILE_NOTE = {
    "full": "no shaping",
    "latency250": "250 ms S3 latency",
    "bw50mbps": "50 Mbps S3 bandwidth",
}
# key, label, unit, higher is better
METRICS = [
    ("durable_import_files_per_sec", "Durable import", "files/s", True),
    ("import_window_p95_fps", "Import window p95", "files/s", True),
    ("metadata_walk_files_per_sec", "Metadata walk", "files/s", True),
    ("cold_read_files_per_sec", "Cold read", "files/s", True),
    ("sequential_cold_read_mib_per_sec", "Cold sequential read", "MiB/s", True),
    ("sequential_cold_read_p10_mib_per_sec", "Cold sequential read p10 (swing floor)", "MiB/s", True),
    ("warm_random_read_iops", "Warm random read", "IOPS", True),
    ("durable_delete_files_per_sec", "Durable delete", "files/s", True),
    ("delete_window_p95_fps", "Delete window p95", "files/s", True),
    ("db_bytes", "Replica DB size", "bytes", False),
    ("db_journal_rows", "Journal rows", "rows", False),
]
# Changes against the base smaller than this are runner noise.
NOISE = 0.15
COMPARABLE = ("seed", "corpus_shape", "corpus_manifest", "corpus_limit", "max_file_bytes", "files", "fanout", "file_size")


def num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def fmt(v):
    if not num(v):
        return "–"
    a = abs(v)
    if a >= 1000:
        return f"{v:,.0f}"
    if a >= 100:
        return f"{v:.0f}"
    if a >= 10:
        return f"{v:.1f}"
    return f"{v:.3g}"


def ratio(v, ref):
    return v / ref if num(v) and num(ref) and ref > 0 else None


def change(v, ref, higher_better):
    """'**🟢 ▲12%**' against the base value; small print within the noise band."""
    r = ratio(v, ref)
    if r is None:
        return "–"
    pct = (r - 1) * 100
    if abs(r - 1) < NOISE:
        return f"<sub>±{abs(pct):.0f}%</sub>"
    good = (pct > 0) == higher_better
    return f"**{'🟢' if good else '🔴'} {'▲' if pct > 0 else '▼'}{abs(pct):.0f}%**"


def geomean(xs):
    xs = [x for x in xs if x is not None and x > 0]
    return math.exp(sum(map(math.log, xs)) / len(xs)) if xs else None


def median(suite, profile):
    return ((suite.get("profiles") or {}).get(profile) or {}).get("median") or {}


def comparable(head, base):
    h, b = head.get("config") or {}, base.get("config") or {}
    return all(h.get(k) == b.get(k) for k in COMPARABLE)


def table(header, rows):
    lines = ["| " + " | ".join(header) + " |", "|---|" + "|".join(["--:"] * (len(header) - 1)) + "|"]
    return "\n".join(lines + ["| " + " | ".join(r) + " |" for r in rows])


def profile_section(profile, head, base):
    h, b = median(head, profile), median(base, profile) if base else {}
    rows = []
    for key, label, unit, hb in METRICS:
        if key not in h and key not in b:
            continue
        row = [f"{label} <sub>{unit}</sub>"]
        if base:
            row.append(fmt(b.get(key)))
        row.append(fmt(h.get(key)))
        if base:
            row.append(change(h.get(key), b.get(key), hb))
        rows.append(row)
    header = ["Metric"] + (["main"] if base else []) + ["this run"] + (["Change"] if base else [])
    body = table(header, rows)
    summary = f"<code>{profile}</code>: {PROFILE_NOTE.get(profile, '')}"
    return f"<details{' open' if profile == 'full' else ''}><summary>{summary}</summary>\n\n{body}\n\n</details>"


def overview(head, base):
    rows = []
    for p in PROFILES:
        if p not in (head.get("profiles") or {}):
            continue
        h, b = median(head, p), median(base, p) if base else {}
        row = [f"`{p}` <sub>{PROFILE_NOTE.get(p, '')}</sub>"]
        if base:
            rs = [(ratio(h.get(k), b.get(k)), hb) for k, _, _, hb in METRICS]
            # Geometric mean of "how much better", so 1.2× is better for every metric.
            g = geomean(r if hb else (1 / r if r else None) for r, hb in rs if r is not None)
            worse = sum(1 for r, hb in rs if r is not None and abs(r - 1) >= NOISE and (r > 1) != hb)
            better = sum(1 for r, hb in rs if r is not None and abs(r - 1) >= NOISE and (r > 1) == hb)
            row += ["–" if g is None else f"{g:.2f}×", f"🟢 {better}", f"🔴 {worse}"]
        else:
            row += [fmt(h.get("durable_import_files_per_sec")), fmt(h.get("cold_read_files_per_sec")), fmt(h.get("warm_random_read_iops"))]
        rows.append(row)
    if base:
        header = ["Profile", "Geomean vs main", "Better", "Worse"]
    else:
        header = ["Profile", "Import files/s", "Cold read files/s", "Warm IOPS"]
    return table(header, rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("head", type=Path)
    ap.add_argument("--base", type=Path)
    ap.add_argument("--meta", default="")
    args = ap.parse_args()

    head = json.loads(args.head.read_text())
    base, note = None, ""
    if args.base and args.base.is_file():
        b = json.loads(args.base.read_text())
        if comparable(head, b):
            base = b
        else:
            note = "main was measured with a different corpus (seed, file limit or size cap), so nothing is compared."
    elif args.base:
        note = "no earlier run on main has results, so nothing is compared."

    cfg = head.get("config") or {}
    corpus = "bundled corpus shape" if cfg.get("corpus_shape") else f"{cfg.get('files')} flat files"
    if cfg.get("corpus_limit"):
        corpus += f", first {cfg['corpus_limit']:,} files"
    if cfg.get("max_file_bytes"):
        corpus += f", files capped at {cfg['max_file_bytes'] >> 10} KiB"
    params = f"{corpus} · seed {cfg.get('seed')} · {cfg.get('repetitions')} repetition(s), median"

    parts = [MARKER, "## Performance regression" + ("" if base else ": no comparison")]
    if args.meta:
        parts.append(f"<sub>{args.meta}</sub>")
    parts.append(f"<sub>{params}</sub>")
    if note:
        parts.append(f"> ℹ️ {note}")
    parts.append(overview(head, base))
    parts.append(
        f"<sub>One import / walk / read / delete cycle per network profile. "
        f"Shared CI runners vary by about ±{NOISE * 100:.0f}%; smaller changes against main are shown in small print. "
        "🟢 better, 🔴 worse (for sizes and row counts, smaller is better).</sub>"
    )
    parts += [profile_section(p, head, base) for p in PROFILES if p in (head.get("profiles") or {})]
    print("\n\n".join(parts))


if __name__ == "__main__":
    sys.exit(main())
