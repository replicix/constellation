#!/usr/bin/env python3
"""Plan 42 section 8: aggregate cell-STORE-VERSION-rN.txt files (cell.sh's output) into tables.
usage: agg.py OUTDIR [--md]
Per store and metric: median and spread (max-min over median) of the runs of base and final, the ratio
final/base oriented so that more than 1 is worse, and a flag when the build is worse than the base by more than the
larger of 3 % and twice the base's spread ("beyond noise")."""
import glob, os, re, statistics, sys
from collections import defaultdict

STORES = ["ext4", "xfs", "btrfs", "btrfs-zstd", "tmpfs"]
HIGHER_BETTER = ("bw_kib", "iops", "ops_per_sec", "pcrace.writes")

def parse_line(line):
    toks = re.findall(r'(\w+)=("[^"]*"|\S+)', line)
    return {k: v.strip('"') for k, v in toks}

def metrics_of(path):
    m, loads, errors = {}, [], []
    for line in open(path):
        line = line.rstrip("\n")
        if line.startswith("LOAD "):
            loads.append(parse_line(line))
        elif line.startswith("ERROR"):
            errors.append(line)
        elif line.startswith("RESULT "):
            parts = line.split()
            name = parts[1]
            kv = parse_line(line)
            tag = ""
            if name == "reopen_pass":
                tag = f".p{kv.get('pass')}"
            elif name == "commit":
                tag = f".v{kv.get('views')}"
            elif name == "pcrace":
                tag = f".{kv.get('variant')}"
            base = name + tag
            for k, v in kv.items():
                if k == "time":
                    w, u, s = (float(x) for x in v.split())
                    m[f"{base}.wall_s"] = w
                    m[f"{base}.cpu_s"] = u + s
                    continue
                if k in ("pass", "views", "variant", "opener", "files", "threads"):
                    continue
                try:
                    m[f"{base}.{k}"] = float(v)
                except ValueError:
                    pass
    return m, loads, errors

OTHERS_MAX = 1.0   # cores used by other work on the host, mean over a phase, above which the phase is "polluted"

def phase_load(path):
    """Per phase of a cell: (name, mean load1, mean cores of other work, max cores of other work)."""
    starts = []
    for line in open(path):
        mt = re.match(r"PHASE (\S+) start (\d+)", line)
        if mt:
            starts.append((mt.group(1), int(mt.group(2))))
        mt = re.match(r"CELL-END .* epoch=(\d+)", line)
        if mt:
            starts.append(("end", int(mt.group(1))))
    samples = []
    try:
        for line in open(path + ".samples"):
            mt = re.match(r"S (\d+) load1=(\S+) running=(\d+) busy=(\S+) mine=(\S+)", line)
            if mt:
                samples.append((int(mt.group(1)), float(mt.group(2)), max(float(mt.group(4)) - float(mt.group(5)), 0.0)))
    except OSError:
        pass
    out = []
    for (name, a), (_, b) in zip(starts, starts[1:]):
        ss = [x for x in samples if a < x[0] <= b + 5]
        if ss:
            out.append((name, sum(x[1] for x in ss) / len(ss), sum(x[2] for x in ss) / len(ss), max(x[2] for x in ss)))
    return out

def interesting(key):
    """Which metrics are reported as times/throughput (the rest are request counts and bookkeeping)."""
    return bool(re.search(r"\.(wall_s|secs|total_secs|write_secs|seen_all_secs)$|_mean_ns$|_p50_ns$|_p99_ns$|"
                          r"_bw_kib$|_iops$|ops_per_sec$|^pcrace.*\.writes$", key))

def higher_better(key):
    return bool(re.search(r"_bw_kib$|_iops$|ops_per_sec$|^pcrace.*\.writes$", key))

def main():
    out = sys.argv[1]
    md = "--md" in sys.argv
    data = defaultdict(lambda: defaultdict(list))   # (store, ver) -> metric -> [values]
    bad = []
    nruns = defaultdict(int)
    phases = {}
    for p in sorted(glob.glob(os.path.join(out, "cell-*.txt"))):
        mt = re.match(r"cell-(.+)-(base|final)-r(\d+)\.txt$", os.path.basename(p))
        if not mt:
            continue
        store, ver, run = mt.group(1), mt.group(2), int(mt.group(3))
        m, loads, errors = metrics_of(p)
        if "CELL-END" not in open(p).read():
            bad.append(f"{store} {ver} r{run}: incomplete")
            continue
        late = [l for l in loads if l.get("gate_ok") == "0"]
        pl = phase_load(p)
        polluted = [f"{n}({o:.1f})" for n, l1, o, mo in pl if o > OTHERS_MAX]
        phases[(store, ver, run)] = pl
        if polluted:
            bad.append(f"{store} {ver} r{run}: other work above {OTHERS_MAX} cores in phase {polluted}")
        mx = max((float(l["load1"]) for l in loads), default=0)
        if late or errors:
            bad.append(f"{store} {ver} r{run}: gate gave up in {[l['phase'] for l in late]} errors={errors[:2]}")
        nruns[(store, ver)] += 1
        for k, v in m.items():
            data[(store, ver)][k].append(v)
        data[(store, ver)]["_maxload"].append(mx)
    rows = []
    for store in STORES:
        a, b = data.get((store, "base")), data.get((store, "final"))
        if not a or not b:
            continue
        for k in sorted(set(a) & set(b)):
            if k.startswith("_"):
                continue
            va, vb = a[k], b[k]
            ma, mb = statistics.median(va), statistics.median(vb)
            sa = (max(va) - min(va)) / ma if ma else 0.0
            sb = (max(vb) - min(vb)) / mb if mb else 0.0
            rows.append((store, k, len(va), ma, sa, len(vb), mb, sb))
    def worse(ma, mb, sa, k):
        if not ma:
            return 0.0
        r = mb / ma
        if higher_better(k):
            r = ma / mb if mb else float("inf")
        return r
    print(f"# cells: " + ", ".join(f"{s}/{v}={n}" for (s, v), n in sorted(nruns.items())))
    if bad:
        print("# INVALID/INCOMPLETE:", *bad, sep="\n#   ")
    flagged = []
    print("\n## time and throughput metrics (median [spread] of the runs; ratio = how much worse final is, >1 worse)")
    print("store | metric | base | final | worse-by | threshold | flag")
    for store, k, na, ma, sa, nb, mb, sb in rows:
        if not interesting(k):
            continue
        w = worse(ma, mb, sa, k)
        thr = max(0.03, 2 * sa)
        fl = "BEYOND-NOISE" if w - 1 > thr else ""
        if fl:
            flagged.append((store, k, w, thr))
        print(f"{store} | {k} | {ma:.6g} [{sa*100:.1f}%] n={na} | {mb:.6g} [{sb*100:.1f}%] n={nb} | {w:.3f} | {thr*100:.1f}% | {fl}")
    print("\n## request counts (median)")
    print("store | metric | base | final | ratio")
    for store, k, na, ma, sa, nb, mb, sb in rows:
        if re.search(r"\.(reqs|n_[a-z]+)$", k) and not interesting(k):
            print(f"{store} | {k} | {ma:.0f} | {mb:.0f} | {(mb / ma if ma else float('nan')):.3f}")
    print("\n## kernel-cache counters (final, median)")
    for store, k, na, ma, sa, nb, mb, sb in rows:
        pass
    print("\n## FLAGGED:", len(flagged))
    for f in flagged:
        print("  ", f)

main()
