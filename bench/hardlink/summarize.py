#!/usr/bin/env python3
"""summarize.py [--compact] OUTDIR...: the cells of abba.sh as markdown tables, per file system.

Per metric and file system: the median over the repetitions of each run, then the median over the runs of each
build. Builds: h0 (the baseline commit), head (the build under test), headnp (head with
CONSTELLATION_FUSE_READDIRPLUS=off, the like-for-like for the listing cells: the baseline predates plan 40's plus
entries). A cell reads `h0 -> head (xR)`, R = head / h0, and, unless --compact, `[lo-hi | lo-hi, n=a/b]`: the
spread of the run values of each build (the noise a ratio is read against) and the number of runs of each.

A cell is read only when its file has a `cell_end` record (an unfinished cell is ignored)."""
import glob
import json
import os
import statistics as st
import sys
from collections import defaultdict

args = [a for a in sys.argv[1:] if not a.startswith("--")]
COMPACT = "--compact" in sys.argv
FS = ["ext4", "xfs", "btrfs", "tmpfs"]
cells = defaultdict(dict)  # (store, ver) -> "dir/run" -> records
for o in args:
    for f in sorted(glob.glob(os.path.join(o, "cell-*.jsonl"))):
        store, ver, r = os.path.basename(f)[5:-6].split("-")
        recs = [json.loads(l) for l in open(f) if l.strip()]
        if any(x["kind"] == "cell_end" for x in recs):
            cells[(store, ver)][os.path.basename(os.path.normpath(o)) + "/" + r] = recs


def runs(store, ver, fn):
    vals = []
    for recs in cells.get((store, ver), {}).values():
        v = fn(recs)
        if v is not None:
            vals.append(v)
    return vals


def med_runs(name, mode):
    def fn(recs):
        w = [x["wall"] for x in recs if x["kind"] == "run" and x["name"] == name and x["mode"] == mode and x["rc"] == 0]
        return st.median(w) if w else None
    return fn


def result(name, *path, scale=1.0):
    def fn(recs):
        for x in recs:
            if x["kind"] == "result" and x["name"] == name:
                if name in ("creates_during_burst", "creates_during_control") and "-h0-" not in x["tag"] \
                        and x.get("rounds", 1) < 3:
                    return None  # the builds under test need the multi-round samples for a p99
                v = x
                for p in path:
                    v = v[p]
                return v * scale
        return None
    return fn


def fmt(x):
    return "%.3g" % x if x < 1000 else "%d" % round(x)


def spread(xs):
    return "%s-%s" % (fmt(min(xs)), fmt(max(xs))) if len(xs) > 1 else fmt(xs[0])


def cell(store, fn, vers, scale=1.0):
    base = runs(store, vers[0], fn)
    if not base:
        return "-"
    out = []
    for v in vers[1:]:
        xs = runs(store, v, fn)
        if not xs:
            out.append("-")
            continue
        a, b = st.median(base) * scale, st.median(xs) * scale
        s = "%s (x%.2f)" % (fmt(b), b / a if a else float("nan"))
        if not COMPACT:
            s += " [%s | %s, n=%d/%d]" % (spread([x * scale for x in base]), spread([x * scale for x in xs]),
                                          len(base), len(xs))
        out.append(s)
    return fmt(st.median(base) * scale) + " -> " + " ; ".join(out)


def table(title, rows, vers=("h0", "head"), scale=1.0):
    print("\n### " + title + "\n")
    print("| | " + " | ".join(FS) + " |")
    print("|---|" + "---|" * len(FS))
    for label, fn in rows:
        print("| " + label + " | " + " | ".join(cell(s, fn, vers, scale) for s in FS) + " |")


print("Cells read: " + ", ".join("%s/%s x%d" % (s, v, len(r)) for (s, v), r in sorted(cells.items())))
table("one `stress-ng link` bogo-op, `meta.db`, remount",
      [("bogo-op wall (s)", result("stress_ng_link_bogo_op", "wall")),
       ("`meta.db` after it (MB)", result("stress_ng_link_bogo_op", "meta_db_bytes", scale=1e-6)),
       ("`.jnl` files after it", result("stress_ng_link_bogo_op", "jnl_files")),
       ("remount after a kill -9 (s)", result("remount_after_crash", "wall")),
       ("remount after a clean unmount of the recovered state (s)", result("remount_clean_after_recovery", "wall"))])
table("concurrent `create` during a burst of 8,192 links / unlinks (us)",
      [("create p99 idle", result("creates_during_burst", "idle", "p99_us")),
       ("create p99 during the links", result("creates_during_burst", "link_burst", "p99_us")),
       ("create p99 during the unlinks", result("creates_during_burst", "unlink_burst", "p99_us")),
       ("create max during the links", result("creates_during_burst", "link_burst", "max_us")),
       ("link p50, the first 100 (n = 2-101)", result("link_unlink_latency", "link_p50_first_100_us")),
       ("link p50 at 2,000 names", result("link_unlink_latency", "link_p50_at_2000_us")),
       ("link p50, the last 100 (n = 8,093-8,192)", result("link_unlink_latency", "link_p50_last_100_us")),
       ("unlink p50, the first 100 (n = 8,192-8,093)", result("link_unlink_latency", "unlink_p50_first_100_us")),
       ("unlink p50 at 2,000 names", result("link_unlink_latency", "unlink_p50_at_2000_names_us")),
       ("burst: links wall (s)", result("creates_during_burst", "link_burst_s"))])
table("control: the same burst with single-name creates and unlinks (us)",
      [("create p99 idle", result("creates_during_control", "idle", "p99_us")),
       ("create p99 during the creates", result("creates_during_control", "link_burst", "p99_us")),
       ("create p99 during the unlinks", result("creates_during_control", "unlink_burst", "p99_us")),
       ("create max during the creates", result("creates_during_control", "link_burst", "max_us"))])

# the gate: the build under test's create p99 during the links / unlinks against its own control's
print("\n### gate: create p99 during the burst / during the control (build under test, ratio of run medians)\n")
print("| | " + " | ".join(FS) + " |")
print("|---|" + "---|" * len(FS))
for label, b, c in (("links / creates", ("creates_during_burst", "link_burst", "p99_us"), ("creates_during_control", "link_burst", "p99_us")),
                    ("unlinks / unlinks", ("creates_during_burst", "unlink_burst", "p99_us"), ("creates_during_control", "unlink_burst", "p99_us"))):
    row = []
    for s in FS:
        xs, cs = runs(s, "head", result(*b)), runs(s, "head", result(*c))
        row.append("%.2f (%s / %s us, n=%d/%d)" % (st.median(xs) / st.median(cs), fmt(st.median(xs)), fmt(st.median(cs)),
                                                   len(xs), len(cs)) if xs and cs else "-")
    print("| " + label + " | " + " | ".join(row) + " |")

rows = []
for n in ("ls_la", "find_ls", "du_s"):
    for m in ("cold", "warm"):
        rows.append(("1,000-file dir `%s` %s (ms)" % (n, m), med_runs("d1k_" + n, m)))
table("nlink-1 directory, 1,000 files (ms): h0 -> head ; headnp", rows, ("h0", "head", "headnp"), 1000.0)
rows = []
for n, lab in (("pnpm_ls_laR", "pnpm-like `ls -laR`"), ("rsnap_ls_laR", "rsnapshot-like gen6 `ls -laR`")):
    for m in ("cold", "warm"):
        rows.append(("%s %s (s)" % (lab, m), med_runs(n, m)))
table("hard-linked trees (s): h0 -> head ; headnp", rows, ("h0", "head", "headnp"))
