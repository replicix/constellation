#!/usr/bin/env python3
import argparse
import json
import os
import statistics
import subprocess
import sys
from pathlib import Path

PROFILES = [
    ("full", {}),
    ("latency250", {"s3_latency_ms": 250}),
    ("bw50mbps", {"s3_bandwidth_mbps": 50}),
]


def run_once(args, label, extra):
    cmd = [
        str(args.harness_bin),
        "bench",
        "--json",
        "--seed",
        str(args.seed),
        "--label",
        label,
    ]
    if args.corpus_shape or args.corpus_manifest:
        if args.corpus_shape:
            cmd.append("--corpus-shape")
        if args.corpus_manifest:
            cmd += ["--corpus-manifest", str(args.corpus_manifest)]
        if args.corpus_limit is not None:
            cmd += ["--corpus-limit", str(args.corpus_limit)]
        if args.max_file_bytes is not None:
            cmd += ["--max-file-bytes", str(args.max_file_bytes)]
    else:
        cmd += [
            "--files",
            str(args.files),
            "--fanout",
            str(args.fanout),
            "--file-size",
            str(args.file_size),
        ]
    if "s3_latency_ms" in extra:
        cmd += ["--s3-latency-ms", str(extra["s3_latency_ms"])]
    if "s3_bandwidth_mbps" in extra:
        cmd += ["--s3-bandwidth-mbps", str(extra["s3_bandwidth_mbps"])]

    env = os.environ.copy()
    env["CONSTELLATION_BIN"] = str(args.constellation_bin)
    p = subprocess.run(cmd, check=False, capture_output=True, text=True, env=env)

    (args.logs_dir / f"{label}.stderr.log").write_text(p.stderr)
    (args.logs_dir / f"{label}.stdout.log").write_text(p.stdout)

    if p.returncode != 0:
        sys.stderr.write(p.stderr)
        if p.stdout.strip():
            sys.stderr.write(p.stdout)
        raise subprocess.CalledProcessError(p.returncode, p.args, p.stdout, p.stderr)

    try:
        return json.loads(p.stdout)
    except json.JSONDecodeError:
        lines = [ln.strip() for ln in p.stdout.splitlines() if ln.strip()]
        for ln in reversed(lines):
            try:
                return json.loads(ln)
            except json.JSONDecodeError:
                continue
        raise RuntimeError(f"No JSON payload found for profile {label}")


def aggregate(reports):
    keys = reports[0].keys()
    out = {}
    for k in keys:
        vals = [r[k] for r in reports]
        if isinstance(vals[0], (int, float)):
            out[k] = statistics.median(vals)
        else:
            out[k] = vals[-1]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--harness-bin", type=Path, required=True)
    ap.add_argument("--constellation-bin", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--logs-dir", type=Path, required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--repetitions", type=int, default=1)
    ap.add_argument(
        "--corpus-shape",
        action="store_true",
        help="Replay the bundled anonymized corpus manifest",
    )
    ap.add_argument("--corpus-manifest", type=Path)
    ap.add_argument("--corpus-limit", type=int)
    ap.add_argument("--max-file-bytes", type=int)
    # Flat synthetic tree (only used when --corpus-shape / --corpus-manifest is off).
    ap.add_argument("--files", type=int, default=20000)
    ap.add_argument("--fanout", type=int, default=400)
    ap.add_argument("--file-size", type=int, default=512)
    args = ap.parse_args()

    args.logs_dir.mkdir(parents=True, exist_ok=True)
    suite = {"config": vars(args).copy(), "profiles": {}}
    suite["config"]["harness_bin"] = str(args.harness_bin)
    suite["config"]["constellation_bin"] = str(args.constellation_bin)
    suite["config"]["out"] = str(args.out)
    suite["config"]["logs_dir"] = str(args.logs_dir)

    for name, extra in PROFILES:
        runs = []
        for i in range(args.repetitions):
            label = f"{name}-r{i+1}"
            runs.append(run_once(args, label, extra))
        suite["profiles"][name] = {
            "median": aggregate(runs),
            "runs": runs,
        }

    args.out.write_text(json.dumps(suite, indent=2))
    print(json.dumps({"out": str(args.out), "profiles": list(suite["profiles"].keys())}))


if __name__ == "__main__":
    main()
