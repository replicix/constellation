#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

METRICS = [
    ("durable_import_files_per_sec", "Durable import files/s", True),
    ("import_window_p95_fps", "Import window p95 files/s", True),
    ("metadata_walk_files_per_sec", "Metadata walk files/s", True),
    ("cold_read_files_per_sec", "Cold read files/s", True),
    ("sequential_cold_read_mib_per_sec", "Cold sequential MiB/s", True),
    ("warm_random_read_iops", "Warm random IOPS", True),
    ("durable_delete_files_per_sec", "Durable delete files/s", True),
    ("delete_window_p95_fps", "Delete window p95 files/s", True),
    ("db_bytes", "Replica DB bytes", False),
    ("db_journal_rows", "Journal rows", False),
]


def fmt(v):
    if isinstance(v, (int, float)):
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        return f"{v:.2f}"
    return str(v)


def pct(head, base, higher_better):
    if base == 0:
        return 0.0
    raw = (head - base) / base * 100.0
    return raw if higher_better else -raw


def icon(score):
    if score <= -10:
        return "🔴"
    if score >= 10:
        return "🟢"
    return "⚪"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--head", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    base = json.loads(args.base.read_text())
    head = json.loads(args.head.read_text())

    lines = []
    lines.append("## Performance Regression Report")
    lines.append("")
    lines.append(
        f"Base: `{base['config']['constellation_bin']}`  |  Head: `{head['config']['constellation_bin']}`"
    )
    lines.append("")

    for profile in ["full", "latency250", "bw50mbps"]:
        b = base["profiles"][profile]["median"]
        h = head["profiles"][profile]["median"]
        lines.append(f"### Profile: `{profile}`")
        lines.append("")
        lines.append("| Metric | Base | Head | Delta |")
        lines.append("|---|---:|---:|---:|")
        for key, label, higher_better in METRICS:
            bv = float(b.get(key, 0.0))
            hv = float(h.get(key, 0.0))
            score = pct(hv, bv, higher_better)
            lines.append(
                f"| {label} | {fmt(bv)} | {fmt(hv)} | {icon(score)} {score:+.1f}% |"
            )
        lines.append("")

    lines.append("Legend: 🟢 >= +10% improvement, 🔴 >= 10% regression, ⚪ within noise band.")
    args.out.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
