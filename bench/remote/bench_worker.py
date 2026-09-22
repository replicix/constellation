#!/usr/bin/env python3
"""Runs ON a mount host (copied there and invoked over ssh by phase_a.py).
stdlib only. One process per node; --threads worker threads inside it.

Each op mode records per-op latency (seconds, monotonic) and errno on
failure, and writes one JSON result file locally on the host, which the
driver fetches over ssh (small file, keeps the slow control link out of
the loop).

Usage:
  bench_worker.py MODE --mount PATH --node NODE --threads N --start-at TS
                   --out /path/out.json [mode-specific args]

Modes: create_empty, small_write, stat_hot, readdir, rename_unlink,
       seq_write, seq_read, visibility_write, visibility_poll, untar, findwc
"""
import argparse
import json
import os
import random
import shutil
import statistics
import subprocess
import sys
import threading
import time


def now():
    return time.monotonic()


def wait_until(ts):
    d = ts - time.time()
    if d > 0:
        time.sleep(d)


def percentiles(xs):
    if not xs:
        return {}
    xs = sorted(xs)
    n = len(xs)
    def p(q):
        i = min(n - 1, int(q * n))
        return xs[i]
    return {
        "p50_ms": p(0.50) * 1000,
        "p90_ms": p(0.90) * 1000,
        "p99_ms": p(0.99) * 1000,
        "max_ms": xs[-1] * 1000,
        "mean_ms": statistics.mean(xs) * 1000,
    }


class OpRecorder:
    def __init__(self):
        self.lat = []
        self.errors = []  # list of {op, errno, msg}
        self.lock = threading.Lock()

    def record(self, dt, err=None):
        with self.lock:
            if err is None:
                self.lat.append(dt)
            else:
                self.errors.append(err)

    def summary(self, duration_s):
        with self.lock:
            n_ok = len(self.lat)
            n_err = len(self.errors)
            out = {
                "ops_ok": n_ok,
                "ops_err": n_err,
                "duration_s": duration_s,
                "ops_per_s": n_ok / duration_s if duration_s > 0 else 0,
                "errors": self.errors[:50],
            }
            out.update(percentiles(self.lat))
            return out


def err_info(e):
    errno = getattr(e, "errno", None)
    return {"errno": errno, "msg": str(e)}


def run_threads(nthreads, fn):
    thread_errors = []

    def guarded(i):
        try:
            fn(i)
        except Exception as e:  # noqa: BLE001 - must not let a thread die silently
            thread_errors.append({"thread": i, "fatal": True, **err_info(e)})

    threads = [threading.Thread(target=guarded, args=(i,)) for i in range(nthreads)]
    t0 = now()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if thread_errors:
        # Surface on stderr immediately (visible in the driver's captured
        # ssh output) as well as via the returned list below.
        sys.stderr.write(f"THREAD FATAL ERRORS: {thread_errors}\n")
    run_threads.last_thread_errors = thread_errors
    return now() - t0


run_threads.last_thread_errors = []


def run_threads_rec(nthreads, fn, rec):
    dur = run_threads(nthreads, fn)
    for e in run_threads.last_thread_errors:
        rec.record(0, e)
    return dur


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

def mode_create_empty(args, rec):
    base = args.dir
    n_per_thread = args.count
    def worker(tid):
        d = base if args.shared else os.path.join(base, f"t{tid}")
        os.makedirs(d, exist_ok=True)
        for i in range(n_per_thread):
            p = os.path.join(d, f"n{args.node}-{tid}-{i}")
            t0 = now()
            try:
                fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                os.close(fd)
                rec.record(now() - t0)
            except OSError as e:
                rec.record(now() - t0, {"op": "create", "path": p, **err_info(e)})
    wait_until(args.start_at)
    dur = run_threads_rec(args.threads, worker, rec)
    return rec.summary(dur)


def mode_small_write(args, rec):
    base = args.dir
    n_per_thread = args.count
    payload = os.urandom(4096)
    def worker(tid):
        d = base if args.shared else os.path.join(base, f"t{tid}")
        os.makedirs(d, exist_ok=True)
        for i in range(n_per_thread):
            p = os.path.join(d, f"sw-n{args.node}-{tid}-{i}")
            t0 = now()
            try:
                fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                os.write(fd, payload)
                os.close(fd)
                rec.record(now() - t0)
            except OSError as e:
                rec.record(now() - t0, {"op": "small_write", "path": p, **err_info(e)})
    wait_until(args.start_at)
    dur = run_threads_rec(args.threads, worker, rec)
    return rec.summary(dur)


def mode_stat_hot(args, rec):
    # args.dir must already contain pre-created files (setup phase).
    base = args.dir
    files = []
    for root, _, fnames in os.walk(base):
        for f in fnames:
            files.append(os.path.join(root, f))
    if not files:
        return {"error": "no files to stat", "dir": base}
    random.shuffle(files)
    n_per_thread = args.count
    def worker(tid):
        for i in range(n_per_thread):
            p = files[(tid * 999331 + i) % len(files)]
            t0 = now()
            try:
                os.stat(p)
                rec.record(now() - t0)
            except OSError as e:
                rec.record(now() - t0, {"op": "stat", "path": p, **err_info(e)})
    wait_until(args.start_at)
    dur = run_threads_rec(args.threads, worker, rec)
    s = rec.summary(dur)
    s["files_available"] = len(files)
    return s


def mode_readdir(args, rec):
    base = args.dir
    n_per_thread = args.count

    def worker(tid):
        for _ in range(n_per_thread):
            t0 = now()
            try:
                n = len(os.listdir(base))
                rec.record(now() - t0)
            except OSError as e:
                rec.record(now() - t0, {"op": "readdir", "path": base, **err_info(e)})
    wait_until(args.start_at)
    dur = run_threads_rec(args.threads, worker, rec)
    return rec.summary(dur)


def mode_rename_unlink(args, rec):
    base = args.dir
    other = args.dir2
    n_per_thread = args.count
    def worker(tid):
        d = os.path.join(base, f"t{tid}")
        d2 = os.path.join(other, f"t{tid}")
        os.makedirs(d, exist_ok=True)
        os.makedirs(d2, exist_ok=True)
        for i in range(n_per_thread):
            src = os.path.join(d, f"r{args.node}-{tid}-{i}")
            dst_same = os.path.join(d, f"r{args.node}-{tid}-{i}-ren")
            dst_cross = os.path.join(d2, f"r{args.node}-{tid}-{i}-cross")
            try:
                open(src, "w").close()
            except OSError:
                continue
            t0 = now()
            try:
                os.rename(src, dst_same)
                rec.record(now() - t0)
            except OSError as e:
                rec.record(now() - t0, {"op": "rename_same_dir", **err_info(e)})
                continue
            t0 = now()
            try:
                os.rename(dst_same, dst_cross)
                rec.record(now() - t0)
            except OSError as e:
                rec.record(now() - t0, {"op": "rename_cross_dir", **err_info(e)})
                continue
            t0 = now()
            try:
                os.unlink(dst_cross)
                rec.record(now() - t0)
            except OSError as e:
                rec.record(now() - t0, {"op": "unlink", **err_info(e)})
    wait_until(args.start_at)
    dur = run_threads_rec(args.threads, worker, rec)
    return rec.summary(dur)


def mode_seq_write(args, rec):
    """One big file per (node,thread), sequential 1 MiB blocks, total
    args.size_mb per thread."""
    block = os.urandom(1024 * 1024)
    n_blocks = args.size_mb
    os.makedirs(args.dir, exist_ok=True)
    def worker(tid):
        p = os.path.join(args.dir, f"seq-n{args.node}-{tid}.bin")
        try:
            fd = os.open(p, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
        except OSError as e:
            rec.record(0, {"op": "open", **err_info(e)})
            return
        for i in range(n_blocks):
            t0 = now()
            try:
                os.write(fd, block)
                rec.record(now() - t0)
            except OSError as e:
                rec.record(now() - t0, {"op": "write", **err_info(e)})
        if args.fsync:
            try:
                os.fsync(fd)
            except OSError:
                pass
        os.close(fd)
    wait_until(args.start_at)
    dur = run_threads_rec(args.threads, worker, rec)
    s = rec.summary(dur)
    total_bytes = s["ops_ok"] * 1024 * 1024
    s["throughput_MBps"] = (total_bytes / (1024 * 1024)) / dur if dur > 0 else 0
    return s


def mode_seq_read(args, rec):
    """Read back files written by mode_seq_write (possibly written by a
    DIFFERENT node/thread -- args.src_node picks whose files to read, for
    the cold-cache-on-another-node case)."""
    block_size = 1024 * 1024
    src_node = args.src_node if args.src_node is not None else args.node
    def worker(tid):
        p = os.path.join(args.dir, f"seq-n{src_node}-{tid}.bin")
        try:
            fd = os.open(p, os.O_RDONLY)
        except OSError as e:
            rec.record(0, {"op": "open", "path": p, **err_info(e)})
            return
        while True:
            t0 = now()
            try:
                data = os.read(fd, block_size)
            except OSError as e:
                rec.record(now() - t0, {"op": "read", **err_info(e)})
                break
            if not data:
                break
            rec.record(now() - t0)
        os.close(fd)
    wait_until(args.start_at)
    dur = run_threads_rec(args.threads, worker, rec)
    s = rec.summary(dur)
    total_bytes = s["ops_ok"] * block_size
    s["throughput_MBps"] = (total_bytes / (1024 * 1024)) / dur if dur > 0 else 0
    return s


def mode_visibility_write(args, rec):
    """Writer side: create/rename/write args.count marker files at a steady
    rate, recording each op's wall-clock creation time so pollers (on other
    nodes) can compute observe-latency."""
    events = []
    d = args.dir
    os.makedirs(d, exist_ok=True)
    wait_until(args.start_at)
    for i in range(args.count):
        p = os.path.join(d, f"vis-{i}")
        t0 = time.time()
        try:
            with open(p, "w") as f:
                f.write(str(i))
                f.flush()
                os.fsync(f.fileno())
            events.append({"i": i, "path": p, "written_at": t0})
        except OSError as e:
            events.append({"i": i, "path": p, "error": str(e)})
        time.sleep(args.interval)
    # NOTE: don't write args.out here -- main() writes the returned dict to
    # args.out after this function returns, which would clobber a write
    # made here. Return the events so they end up in that final write.
    return {"events_written": len(events), "events": events}


def mode_visibility_poll(args, rec):
    """Poller side: given the writer's event list (via --events-file, fetched
    by the driver from the writer's --out and copied here), poll for each
    marker's existence + content, recording observe latency."""
    with open(args.events_file) as f:
        writer_data = json.load(f)
    events = writer_data["events"]
    results = []
    deadline = time.time() + args.poll_timeout
    for ev in events:
        if "error" in ev:
            continue
        p = ev["path"].replace(args.writer_dir, args.dir) if args.writer_dir else ev["path"]
        expected = str(ev["i"])
        t_start = time.time()
        seen_at = None
        while time.time() < deadline:
            try:
                with open(p) as f:
                    content = f.read()
                if content == expected:
                    seen_at = time.time()
                    break
            except (FileNotFoundError, OSError):
                pass
            time.sleep(0.02)
        if seen_at is not None:
            results.append({"i": ev["i"], "latency_s": seen_at - ev["written_at"]})
        else:
            results.append({"i": ev["i"], "latency_s": None, "timeout": True})
    lat = [r["latency_s"] for r in results if r["latency_s"] is not None]
    out = percentiles(lat) if lat else {}
    out["samples"] = len(results)
    out["timeouts"] = sum(1 for r in results if r.get("timeout"))
    out["raw"] = results
    return out


def mode_untar(args, rec):
    t0 = now()
    try:
        subprocess.run(
            ["tar", "xf", args.tarball, "-C", args.dir],
            check=True, capture_output=True, timeout=args.timeout_s,
        )
        dt = now() - t0
    except subprocess.CalledProcessError as e:
        return {"error": "tar failed", "stderr": e.stderr.decode(errors="replace")[-2000:]}
    except subprocess.TimeoutExpired:
        return {"error": "tar timed out"}
    return {"untar_s": dt}


def mode_findwc(args, rec):
    t0 = now()
    n_files = 0
    n_dirs = 0
    total_size = 0
    for root, dirs, files in os.walk(args.dir):
        n_dirs += len(dirs)
        n_files += len(files)
        for fn in files:
            try:
                total_size += os.path.getsize(os.path.join(root, fn))
            except OSError:
                pass
    find_s = now() - t0
    t0 = now()
    du_s = None
    try:
        p = subprocess.run(["du", "-sb", args.dir], capture_output=True, text=True, timeout=120)
        du_s = now() - t0
        du_bytes = int(p.stdout.split()[0]) if p.stdout else None
    except Exception:
        du_bytes = None
    return {
        "find_s": find_s, "n_files": n_files, "n_dirs": n_dirs,
        "walked_total_size": total_size, "du_s": du_s, "du_bytes": du_bytes,
    }


MODES = {
    "create_empty": mode_create_empty,
    "small_write": mode_small_write,
    "stat_hot": mode_stat_hot,
    "readdir": mode_readdir,
    "rename_unlink": mode_rename_unlink,
    "seq_write": mode_seq_write,
    "seq_read": mode_seq_read,
    "visibility_write": mode_visibility_write,
    "visibility_poll": mode_visibility_poll,
    "untar": mode_untar,
    "findwc": mode_findwc,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=MODES.keys())
    ap.add_argument("--mount", required=True)
    ap.add_argument("--dir", default=None, help="working dir under mount (defaults to mount)")
    ap.add_argument("--dir2", default=None, help="second dir, for cross-dir rename")
    ap.add_argument("--node", default="0")
    ap.add_argument("--threads", type=int, default=1)
    ap.add_argument("--count", type=int, default=100)
    ap.add_argument("--start-at", type=float, default=0.0)
    ap.add_argument("--out", required=True)
    ap.add_argument("--shared", action="store_true")
    ap.add_argument("--size-mb", type=int, default=100)
    ap.add_argument("--fsync", action="store_true")
    ap.add_argument("--src-node", default=None)
    ap.add_argument("--interval", type=float, default=0.05)
    ap.add_argument("--poll-timeout", type=float, default=30.0)
    ap.add_argument("--events-file", default=None)
    ap.add_argument("--writer-dir", default=None)
    ap.add_argument("--tarball", default=None)
    ap.add_argument("--timeout-s", type=float, default=600.0)
    args = ap.parse_args()

    if args.dir is None:
        args.dir = args.mount
    if args.dir2 is None:
        args.dir2 = args.mount

    rec = OpRecorder()
    fn = MODES[args.mode]
    try:
        result = fn(args, rec)
    except Exception as e:
        result = {"fatal_error": str(e)}
    result["mode"] = args.mode
    result["node"] = args.node
    result["threads"] = args.threads
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result.get("errors", []) and {"errors_sample": result["errors"][:3]} or {"ok": True}))


if __name__ == "__main__":
    main()
