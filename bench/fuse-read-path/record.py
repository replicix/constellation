#!/usr/bin/env python3
"""Compose one JSONL result line from a cell's raw directory.

usage: record.py CELL_DIR key=value...

Reads fio.json, stats.json (chunkfs), procstat/diskstats/pio .before/.after,
resident.before/.after, verify.log, read_ahead_kb from CELL_DIR.
"""
import json
import os
import platform
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))


def read(path, default=None):
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return default


def cpu_busy_pct(before, after):
    def parse(text):
        f = [int(x) for x in text.splitlines()[0].split()[1:]]
        idle = f[3] + (f[4] if len(f) > 4 else 0)
        return sum(f[:8]), idle
    if not before or not after:
        return None
    t0, i0 = parse(before)
    t1, i1 = parse(after)
    return round(100.0 * (1 - (i1 - i0) / max(t1 - t0, 1)), 2)


def disk_read_mib(before, after):
    def parse(text):
        out = {}
        for line in text.splitlines():
            p = line.split()
            if os.path.exists(f"/sys/block/{p[2]}") and not p[2].startswith(("loop", "ram", "zram", "dm-", "md")):
                out[p[2]] = int(p[5])
        return out
    if not before or not after:
        return None
    b, a = parse(before), parse(after)
    return round(sum(a[k] - b.get(k, 0) for k in a) * 512 / 2**20, 1)


def pio_read_mib(before, after):
    def parse(text):
        return {l.split(":")[0]: int(l.split(":")[1]) for l in text.splitlines() if ":" in l}
    if not before or not after:
        return None
    return round((parse(after).get("read_bytes", 0) - parse(before).get("read_bytes", 0)) / 2**20, 1)


def resident_pct(text):
    if not text:
        return None
    r, s = (int(x) for x in text.split())
    return round(100.0 * r / s, 1) if s > 0 else None


def cmd_out(*args):
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=10).stdout.strip()
    except Exception:
        return None


def git_commit(path):
    c = read(os.path.join(HERE, "build", path))
    return c.strip() if c else None


def main():
    d = sys.argv[1]
    meta = dict(a.split("=", 1) for a in sys.argv[2:])
    rec = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "host": platform.node(),
        "kernel": platform.release(),
        "libfuse_commit": git_commit("libfuse-zc.commit" if meta.get("mode") == "uring-zc" else "libfuse.commit"),
        "fio_version": cmd_out(os.path.join(HERE, "build", "fio"), "--version"),
        "nproc": os.cpu_count(),
        "mem_gib": round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1),
    }
    store = meta.get("store", "")
    rec["store_fs"] = cmd_out("stat", "-f", "-c", "%T", store) if store else None
    rec["enable_uring"] = (read("/sys/module/fuse/parameters/enable_uring") or "").strip() or None
    rec["pipe_max_size"] = int((read("/proc/sys/fs/pipe-max-size") or "0").strip())
    rec["euid"] = os.geteuid()
    for k in ("rep", "quick", "runtime", "chunk_size"):
        if k in meta and meta[k] != "":
            meta[k] = int(meta[k])
    rec.update(meta)
    ra = read(os.path.join(d, "read_ahead_kb"))
    rec["read_ahead_kb"] = int(ra) if ra else None
    rec["verify"] = (read(os.path.join(d, "verify.log")) or "").strip().splitlines()

    fio = None
    txt = read(os.path.join(d, "fio.json"))
    if txt:
        try:
            fio = json.loads(txt[txt.index("{"):])
        except (ValueError, json.JSONDecodeError):
            fio = None
    if fio:
        job = fio["jobs"][0]
        r = job["read"]
        pct = r.get("clat_ns", {}).get("percentile", {})
        rec["fio"] = {
            "bw_mib_s": round(r["bw_bytes"] / 2**20, 1),
            "iops": round(r["iops"], 1),
            "io_mib": round(r["io_bytes"] / 2**20, 1),
            "runtime_s": round(r["runtime"] / 1000, 3),
            "lat_mean_us": round(r["lat_ns"]["mean"] / 1000, 2),
            "clat_p50_us": round(pct.get("50.000000", 0) / 1000, 2),
            "clat_p99_us": round(pct.get("99.000000", 0) / 1000, 2),
            "usr_cpu_pct": round(job["usr_cpu"], 2),
            "sys_cpu_pct": round(job["sys_cpu"], 2),
        }
    elif rec.get("status") == "ok":
        rec["status"] = "fio_failed"

    rec["sys_cpu_busy_pct"] = cpu_busy_pct(read(os.path.join(d, "procstat.before")),
                                           read(os.path.join(d, "procstat.after")))
    rec["disk_read_mib"] = disk_read_mib(read(os.path.join(d, "diskstats.before")),
                                         read(os.path.join(d, "diskstats.after")))
    rec["daemon_storage_read_mib"] = pio_read_mib(read(os.path.join(d, "pio.before")),
                                                  read(os.path.join(d, "pio.after")))
    rec["backing_resident_before_pct"] = resident_pct(read(os.path.join(d, "resident.before")))
    rec["backing_resident_after_pct"] = resident_pct(read(os.path.join(d, "resident.after")))

    st = read(os.path.join(d, "stats.json"))
    if st:
        s = json.loads(st)
        w = s["window"] if s.get("window_valid") else s["total"]
        n = s["negotiated"]
        rec["daemon"] = {
            "window_valid": bool(s.get("window_valid")),
            "utime_s": w["utime_s"], "stime_s": w["stime_s"], "cpu_s": w["cpu_s"],
            "cpu_s_per_gib": w["cpu_s_per_gib"],
            "maxrss_kib": s["rss"]["hwm_kib"], "rss_anon_kib": s["rss"]["rss_anon_kib"],
            "rss_file_kib": s["rss"]["rss_file_kib"],
            "memcache_mib": round(s["memcache"]["bytes"] / 2**20, 1),
            "reads": w["reads"], "gib_served": round(w["bytes"] / 2**30, 3),
            "uring_reads": w["uring_reads"], "fallbacks": w["fallbacks"], "errors": w["errors"],
            "memcache_over_limit": w["memcache_over_limit"], "zc_reads": w["zc_reads"],
            "zc_spanning_whole": w["zc_spanning_whole"],
            "writev_calls": w["writev_calls"], "splice_calls": w["splice_calls"],
            "splice_move_calls": w["splice_move_calls"], "vmsplice_calls": w["vmsplice_calls"],
            "vmsplice_gift_calls": w["vmsplice_gift_calls"],
            "req_size_min": w["req_size_min"], "req_size_max": w["req_size_max"],
            "req_size_hist": w["req_size_hist"],
            "req_size_p50": hist_p50(w["req_size_hist"]),
            "pipe_probe": s.get("pipe_probe"),
        }
        rec["negotiated"] = {k: n[k] for k in ("io_uring", "io_uring_bufpool", "passthrough", "splice_write",
                                               "splice_move", "max_write", "max_read", "max_readahead",
                                               "max_pages", "want")}
        rec["negotiated"]["init_status"] = [l for l in n["libfuse_log"].splitlines()
                                            if "io_uring" in l or "uring" in l]
        if s.get("init_error"):
            rec["error"] = rec.get("error") or s["init_error"]
        rec["checks"] = checks(rec, w, n)
    print(json.dumps(rec, sort_keys=False))


def hist_p50(h):
    total = sum(h.values())
    acc = 0
    for k, v in h.items():
        acc += v
        if acc * 2 >= total:
            return k
    return None


def checks(rec, w, n):
    """Warnings that mean a cell does not measure what its mode name claims."""
    out = []
    mode = rec.get("mode", "")
    if w["errors"]:
        out.append(f"{w['errors']} read errors")
    if mode.startswith("uring") and w["reads"] and w["uring_reads"] < w["reads"]:
        out.append(f"only {w['uring_reads']}/{w['reads']} reads came over io_uring")
    if mode == "uring-zc" and w["reads"] and w["zc_reads"] < w["reads"]:
        out.append(f"only {w['zc_reads']}/{w['reads']} reads were zero-copy")
    if mode == "uring-bufpool" and not n["io_uring_bufpool"]:
        out.append("bufpool not negotiated")
    if mode == "passthrough" and w["reads"]:
        out.append(f"{w['reads']} reads reached the daemon despite passthrough")
    if w["reads"] and w["fallbacks"]:
        out.append(f"{w['fallbacks']}/{w['reads']} replies fell back to a copy path")
    if mode == "memcache" and rec.get("cache") == "warm" and (rec.get("daemon_storage_read_mib") or 0) > 1:
        out.append("warm memcache read from storage")
    if rec.get("cache") == "warm" and (rec.get("disk_read_mib") or 0) > 64:
        out.append(f"warm run read {rec['disk_read_mib']} MiB from disk")
    return out


if __name__ == "__main__":
    main()
