#!/usr/bin/env python3
"""Phase A real-world benchmark driver. python3 stdlib only.

Usage: python3 phase_a.py <prefix> <results_dir> [rows...]
  rows: space separated subset of {1,2,3,4,5,6,7,e2e}; default = all.

Expects the plain FS to be named PLAIN_NAME and (for the e2e row) an E2E
FS named E2E_NAME, both already fs-create'd + mounted on every node used
(see setup_plain()/setup_e2e() below, called automatically if not already
mounted).
"""
import json
import os
import sys
import time

import common as c

RESULTS_DIR = None
PLAIN_NAME = "bench-plain"
E2E_NAME = "bench-e2e"
E2E_PASS = "verify-e2e-bench-pass-9f2c"
BARRIER_DELAY = 8.0  # seconds of buffer for ssh fan-out before workers start

ALL_NODES = ["a", "b", "c", "d"]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def save(obj, name):
    path = os.path.join(RESULTS_DIR, name)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, default=str)
    log(f"  saved {path}")


def ensure_mounted(nodes, name, prefix, write_mode="through", e2e_pass=None):
    st = c.status_fanout(nodes, name, timeout=10)
    need = [n for n in nodes if not st[n].ok()]
    if not need:
        log(f"{name}: already mounted on {nodes}")
        return
    log(f"{name}: mounting on {need}")
    if need[0] == nodes[0] and c.status(nodes[0], name, timeout=5).returncode != 0:
        # fs may not exist yet at the backend -- create once from the first node
        r = c.fs_create(nodes[0], prefix, name, e2e=bool(e2e_pass), e2e_passphrase=e2e_pass, timeout=60)
        log(f"  fs_create({nodes[0]}): rc={r.returncode} {r.stdout.strip()[:200]!r} {r.stderr.strip()[-300:]!r}")
    res = c.mount_fanout(need, name, prefix=prefix, write_mode=write_mode, e2e_passphrase=e2e_pass, timeout=90)
    for n, r in res.items():
        ok = r.ok()
        log(f"  mount({n}): ok={ok} out={r.stdout.strip()!r} err={r.stderr.strip()[-300:]!r}")
        if not ok:
            raise RuntimeError(f"mount failed on {n}: {r.stderr}")
    time.sleep(2)


def run_row(nodes, name, mode, extra_args_per_node=None, extra_args="", out_suffix="", timeout=600):
    """Run bench_worker `mode` on every node in `nodes` concurrently with a
    wall-clock barrier, fetch+aggregate results."""
    start_at = time.time() + BARRIER_DELAY
    out_paths = {n: f"/home/ubuntu/cbench/out-{mode}{out_suffix}-{n}.json" for n in nodes}
    results = {}
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as ex:
        futs = {}
        for n in nodes:
            ea = extra_args
            if extra_args_per_node and n in extra_args_per_node:
                ea = ea + " " + extra_args_per_node[n]
            futs[ex.submit(c.run_worker, n, mode, name, start_at, out_paths[n], ea, timeout)] = n
        for fut in concurrent.futures.as_completed(futs):
            n = futs[fut]
            r = fut.result()
            if not r.ok():
                log(f"  WORKER FAILED on {n}: rc={r.returncode} timeout={r.timed_out} tail={r.stdout[-500:]!r} {r.stderr[-500:]!r}")
    for n in nodes:
        j = c.fetch_worker_result(n, out_paths[n])
        results[n] = j
    return results


def aggregate_ops(results):
    """Sum ops_ok/ops_err across nodes, merge latency percentiles by
    reporting per-node and an overall max p99/max."""
    total_ok = sum(r.get("ops_ok", 0) for r in results.values() if r)
    total_err = sum(r.get("ops_err", 0) for r in results.values() if r)
    max_p99 = max((r.get("p99_ms", 0) for r in results.values() if r), default=None)
    max_dur = max((r.get("duration_s", 0) for r in results.values() if r), default=0)
    agg_ops_per_s = total_ok / max_dur if max_dur > 0 else 0
    errors_sample = []
    for r in results.values():
        if r and r.get("errors"):
            errors_sample.extend(r["errors"][:5])
    return {
        "total_ops_ok": total_ok, "total_ops_err": total_err,
        "aggregate_ops_per_s": agg_ops_per_s, "max_p99_ms": max_p99,
        "errors_sample": errors_sample[:20],
        "per_node": results,
    }


def row1_create_empty(nodes, threads, count, shared):
    label = f"row1_create_empty_n{len(nodes)}_t{threads}_{'shared' if shared else 'disjoint'}"
    log(f"=== {label} ===")
    # NOTE: each row config gets its own subdir -- reusing one directory
    # across repeated invocations (e.g. running the 1-node then 4-node
    # variant) would make node "a" re-create the exact same filenames it
    # made in the 1-node pass and see spurious EEXIST. That's a harness
    # naming artifact, not a filesystem bug.
    d = f"/create-shared-n{len(nodes)}" if shared else f"/create-disjoint-n{len(nodes)}"
    extra = f"--threads {threads} --count {count} --dir {c.mount_point_for(PLAIN_NAME)}{d}" + (" --shared" if shared else "")
    res = run_row(nodes, PLAIN_NAME, "create_empty", extra_args=extra, out_suffix=label)
    agg = aggregate_ops(res)
    save(agg, f"{label}.json")
    log(f"  {label}: ok={agg['total_ops_ok']} err={agg['total_ops_err']} agg_ops/s={agg['aggregate_ops_per_s']:.1f} p99_max={agg['max_p99_ms']}")
    return agg


def row2_small_write(nodes, threads, count, shared, name, write_mode):
    label = f"row2_small_write_{name}_{write_mode}_n{len(nodes)}_t{threads}_{'shared' if shared else 'disjoint'}"
    log(f"=== {label} ===")
    c.set_write_mode_fanout(nodes, name, write_mode, timeout=30)
    time.sleep(1)
    d = f"/sw-shared-n{len(nodes)}-{write_mode}" if shared else f"/sw-disjoint-n{len(nodes)}-{write_mode}"
    extra = f"--threads {threads} --count {count} --dir {c.mount_point_for(name)}{d}" + (" --shared" if shared else "")
    res = run_row(nodes, name, "small_write", extra_args=extra, out_suffix=label)
    agg = aggregate_ops(res)
    save(agg, f"{label}.json")
    log(f"  {label}: ok={agg['total_ops_ok']} err={agg['total_ops_err']} agg_ops/s={agg['aggregate_ops_per_s']:.1f} p99_max={agg['max_p99_ms']}")
    return agg


def row3_metadata(nodes, threads, name=PLAIN_NAME):
    # hot stat: reuse files from row1 shared create (if present) else create some.
    label_stat = f"row3_stat_hot_n{len(nodes)}_t{threads}"
    log(f"=== {label_stat} ===")
    statdir = c.mount_point_for(name) + "/create-shared"
    extra = f"--threads {threads} --count 300 --dir {statdir}"
    res = run_row(nodes, name, "stat_hot", extra_args=extra, out_suffix=label_stat)
    agg = aggregate_ops(res)
    save(agg, f"{label_stat}.json")
    log(f"  {label_stat}: ok={agg['total_ops_ok']} err={agg['total_ops_err']} agg_ops/s={agg['aggregate_ops_per_s']:.1f}")

    # readdir of a 10k-entry directory: build it once from node a, then all nodes readdir.
    label_rd = f"row3_readdir10k_n{len(nodes)}_t{threads}"
    log(f"=== {label_rd} ===")
    bigdir = c.mount_point_for(name) + "/dir10k"
    mk = c.ssh_run(nodes[0], f"mkdir -p {bigdir} && cd {bigdir} && "
                              f"python3 -c \"import os;[open(f'f{i}','w').close() for i in range(10000) if not os.path.exists(f'f{i}')]\"",
                   timeout=180)
    log(f"  populate 10k dir on {nodes[0]}: rc={mk.returncode} {mk.stderr[-300:]!r}")
    time.sleep(2)
    extra = f"--threads {threads} --count 20 --dir {bigdir}"
    res = run_row(nodes, name, "readdir", extra_args=extra, out_suffix=label_rd)
    agg = aggregate_ops(res)
    save(agg, f"{label_rd}.json")
    log(f"  {label_rd}: ok={agg['total_ops_ok']} err={agg['total_ops_err']} agg_ops/s={agg['aggregate_ops_per_s']:.1f}")
    return {"stat_hot": agg, "readdir10k": agg}


def row4_rename_unlink(nodes, threads, name=PLAIN_NAME):
    label = f"row4_rename_unlink_n{len(nodes)}_t{threads}"
    log(f"=== {label} ===")
    d1 = c.mount_point_for(name) + "/ren-a"
    d2 = c.mount_point_for(name) + "/ren-b"
    extra = f"--threads {threads} --count 200 --dir {d1} --dir2 {d2}"
    res = run_row(nodes, name, "rename_unlink", extra_args=extra, out_suffix=label)
    agg = aggregate_ops(res)
    save(agg, f"{label}.json")
    log(f"  {label}: ok={agg['total_ops_ok']} err={agg['total_ops_err']} agg_ops/s={agg['aggregate_ops_per_s']:.1f}")
    return agg


def row5_seq(nodes, threads, size_mb, name, write_mode):
    label_w = f"row5_seqwrite_{name}_{write_mode}_n{len(nodes)}_t{threads}"
    log(f"=== {label_w} (size={size_mb}MiB/thread) ===")
    c.set_write_mode_fanout(nodes, name, write_mode, timeout=30)
    time.sleep(1)
    d = c.mount_point_for(name) + "/seq"
    extra = f"--threads {threads} --dir {d} --size-mb {size_mb} --fsync"
    res = run_row(nodes, name, "seq_write", extra_args=extra, out_suffix=label_w, timeout=900)
    agg_w = aggregate_ops(res)
    save(agg_w, f"{label_w}.json")
    thr = [r.get("throughput_MBps", 0) for r in res.values() if r]
    log(f"  {label_w}: per-node throughput MB/s={thr} err={agg_w['total_ops_err']}")

    # cold-cache read: each node reads a DIFFERENT node's file (rotate by 1).
    quiesced, _ = c.wait_quiesce(nodes, name, timeout_s=180, poll_s=3)
    log(f"  quiesce before cross-node read: {quiesced}")
    label_r = f"row5_seqread_{name}_{write_mode}_n{len(nodes)}_t{threads}"
    log(f"=== {label_r} ===")
    node_list = nodes
    src_map = {n: node_list[(i + 1) % len(node_list)] for i, n in enumerate(node_list)} if len(node_list) > 1 else {node_list[0]: node_list[0]}
    extra_per = {n: f"--src-node {src_map[n]}" for n in nodes}
    extra = f"--threads {threads} --dir {d} --size-mb {size_mb}"
    res2 = run_row(nodes, name, "seq_read", extra_args=extra, extra_args_per_node=extra_per, out_suffix=label_r, timeout=900)
    agg_r = aggregate_ops(res2)
    save(agg_r, f"{label_r}.json")
    thr2 = [r.get("throughput_MBps", 0) for r in res2.values() if r]
    log(f"  {label_r}: per-node throughput MB/s={thr2} err={agg_r['total_ops_err']} src_map={src_map}")
    return {"write": agg_w, "read": agg_r, "src_map": src_map}


def row6_visibility(nodes, name=PLAIN_NAME, count=210):
    label = f"row6_visibility_n{len(nodes)}"
    log(f"=== {label} ===")
    writer = nodes[0]
    pollers = nodes[1:]
    d = c.mount_point_for(name) + "/vis"
    wout = f"/home/ubuntu/cbench/out-vis-writer.json"
    wr_extra = f"--dir {d} --count {count} --interval 0.05"
    # Writer runs to completion first (events file is on ITS local disk,
    # not the shared mount -- pollers on other hosts can't read it directly,
    # so it must be fetched locally and pushed out before polling starts).
    start_at = time.time() + BARRIER_DELAY
    wr = c.run_worker(writer, "visibility_write", name, start_at, wout, wr_extra, 120)
    if not wr.ok():
        log(f"  visibility writer failed: {wr.stdout[-300:]!r} {wr.stderr[-300:]!r}")
    wev = c.fetch_worker_result(writer, wout)
    local_events = os.path.join(RESULTS_DIR, "vis_events.json")
    with open(local_events, "w") as f:
        json.dump(wev, f)
    for n in pollers:
        pr = c.scp_to(n, local_events, "/home/ubuntu/cbench/vis_events_in.json", timeout=30)
        if not pr.ok():
            log(f"  scp events to {n} failed: {pr.stderr}")

    start_at2 = time.time() + BARRIER_DELAY
    pout_paths = {}
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(pollers)) as ex:
        futs = {}
        for n in pollers:
            pout = f"/home/ubuntu/cbench/out-vis-poll-{n}.json"
            pout_paths[n] = pout
            pextra = f"--dir {d} --events-file /home/ubuntu/cbench/vis_events_in.json --poll-timeout 30"
            futs[ex.submit(c.run_worker, n, "visibility_poll", name, start_at2, pout, pextra, 120)] = n
        for fut in concurrent.futures.as_completed(futs):
            n = futs[fut]
            r = fut.result()
            if not r.ok():
                log(f"  visibility poller failed on {n}: {r.stdout[-300:]!r} {r.stderr[-300:]!r}")
    results = {}
    for n in pollers:
        pj = c.fetch_worker_result(n, pout_paths[n])
        results[n] = pj
    agg = {"writer_events": wev, "poller_results": results}
    save(agg, f"{label}.json")
    for n, pj in results.items():
        if pj:
            log(f"  visibility {n}: p50={pj.get('p50_ms')} p90={pj.get('p90_ms')} p99={pj.get('p99_ms')} max={pj.get('max_ms')} samples={pj.get('samples')} timeouts={pj.get('timeouts')}")
    return agg


def main():
    global RESULTS_DIR
    prefix = sys.argv[1]
    RESULTS_DIR = sys.argv[2]
    rows = sys.argv[3:] or ["1", "2", "3", "4", "5", "6", "7"]
    os.makedirs(RESULTS_DIR, exist_ok=True)

    log("deploying bench_worker.py to all nodes")
    dep = c.deploy_worker(ALL_NODES, os.path.join(os.path.dirname(__file__), "bench_worker.py"))
    for n, r in dep.items():
        if not r.ok():
            log(f"  DEPLOY FAILED {n}: {r.stderr}")

    if any(r in rows for r in ["1", "2", "3", "4", "5", "6", "7"]):
        ensure_mounted(ALL_NODES, PLAIN_NAME, prefix, write_mode="through")

    summary = {}

    if "1" in rows:
        summary["row1_1node_shared"] = row1_create_empty(["a"], 8, 300, True)
        summary["row1_1node_disjoint"] = row1_create_empty(["a"], 8, 300, False)
        summary["row1_4node_shared"] = row1_create_empty(ALL_NODES, 8, 150, True)
        summary["row1_4node_disjoint"] = row1_create_empty(ALL_NODES, 8, 150, False)

    if "2" in rows:
        for wm in ["through", "back"]:
            summary[f"row2_1node_shared_{wm}"] = row2_small_write(["a"], 8, 200, True, PLAIN_NAME, wm)
            summary[f"row2_1node_disjoint_{wm}"] = row2_small_write(["a"], 8, 200, False, PLAIN_NAME, wm)
            summary[f"row2_4node_shared_{wm}"] = row2_small_write(ALL_NODES, 8, 100, True, PLAIN_NAME, wm)
            summary[f"row2_4node_disjoint_{wm}"] = row2_small_write(ALL_NODES, 8, 100, False, PLAIN_NAME, wm)
        c.set_write_mode_fanout(ALL_NODES, PLAIN_NAME, "through", timeout=30)

    if "3" in rows:
        summary["row3_1node"] = row3_metadata(["a"], 8)
        summary["row3_4node"] = row3_metadata(ALL_NODES, 8)

    if "4" in rows:
        summary["row4_1node"] = row4_rename_unlink(["a"], 8)
        summary["row4_4node"] = row4_rename_unlink(ALL_NODES, 8)

    if "5" in rows:
        for wm in ["through", "back"]:
            summary[f"row5_1node_{wm}"] = row5_seq(["a"], 1, int(os.environ.get("SEQ_MB", "512")), PLAIN_NAME, wm)
            if len(ALL_NODES) > 1:
                summary[f"row5_4node_{wm}"] = row5_seq(ALL_NODES, 1, int(os.environ.get("SEQ_MB", "512")), PLAIN_NAME, wm)
        c.set_write_mode_fanout(ALL_NODES, PLAIN_NAME, "through", timeout=30)

    if "6" in rows:
        summary["row6_visibility"] = row6_visibility(ALL_NODES, PLAIN_NAME, count=210)

    if "7" in rows:
        summary["row7_untar"] = row7_untar(ALL_NODES)

    if "e2e" in rows:
        ensure_mounted(ALL_NODES, E2E_NAME, prefix + "-e2e", write_mode="through", e2e_pass=E2E_PASS)
        summary["e2e_row1_1node_shared"] = row1_create_empty_named(["a"], 8, 300, True, E2E_NAME)
        summary["e2e_row2_1node_disjoint"] = row2_small_write(["a"], 8, 200, False, E2E_NAME, "through")

    save(summary, "phase_a_summary.json")
    log("DONE")


def row1_create_empty_named(nodes, threads, count, shared, name):
    label = f"row1_create_empty_{name}_n{len(nodes)}_t{threads}_{'shared' if shared else 'disjoint'}"
    log(f"=== {label} ===")
    d = "/create-shared" if shared else "/create-disjoint"
    extra = f"--threads {threads} --count {count} --dir {c.mount_point_for(name)}{d}" + (" --shared" if shared else "")
    res = run_row(nodes, name, "create_empty", extra_args=extra, out_suffix=label)
    agg = aggregate_ops(res)
    save(agg, f"{label}.json")
    log(f"  {label}: ok={agg['total_ops_ok']} err={agg['total_ops_err']} agg_ops/s={agg['aggregate_ops_per_s']:.1f}")
    return agg


def row7_untar(nodes):
    label = f"row7_untar_n{len(nodes)}"
    log(f"=== {label} ===")
    tarball_url = os.environ.get("TARBALL_URL", "https://ftp.gnu.org/gnu/coreutils/coreutils-9.5.tar.gz")
    tarball_local = "/home/ubuntu/cbench/src.tar.gz"
    dl = c.ssh_fanout(nodes, f"[ -f {tarball_local} ] || curl -fsSL {tarball_url} -o {tarball_local}", timeout=120)
    for n, r in dl.items():
        log(f"  download {n}: rc={r.returncode} err={r.stderr[-200:]!r}")
    d = c.mount_point_for(PLAIN_NAME) + "/untar"
    for n in nodes:
        c.ssh_run(n, f"mkdir -p {d}-{n}", timeout=15)
    res = run_row(nodes, PLAIN_NAME, "untar", extra_args_per_node={n: f"--dir {d}-{n} --tarball {tarball_local} --timeout-s 300" for n in nodes}, out_suffix=label, timeout=320)
    save(res, f"{label}_untar.json")
    for n, r in res.items():
        log(f"  untar {n}: {r}")

    label2 = f"row7_findwc_n{len(nodes)}"
    res2 = run_row(nodes, PLAIN_NAME, "findwc", extra_args_per_node={n: f"--dir {d}-{n}" for n in nodes}, out_suffix=label2, timeout=180)
    save(res2, f"{label2}.json")
    for n, r in res2.items():
        log(f"  findwc {n}: {r}")
    return {"untar": res, "findwc": res2}


if __name__ == "__main__":
    main()
