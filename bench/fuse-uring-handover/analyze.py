#!/usr/bin/env python3
"""Turn one run directory from run.sh into a JSON line (or, with --human, JSON
lines on stdin into one readable line each).

    analyze.py results/<kernel>/v2-r1          -> JSON line
    analyze.py --human < summary.jsonl         -> one line per run
    analyze.py --table results/*/summary.jsonl  -> one readable line per run
    analyze.py --matrix results/*/summary.jsonl -> markdown tables, kernel x variant
"""
import glob
import json
import os
import re
import sys

ERRNO = {103: "ECONNABORTED", 107: "ENOTCONN", 125: "ECANCELED", 5: "EIO",
         4: "EINTR", 19: "ENODEV", 14: "EFAULT", 0: "-"}


def stats(path):
    d = {}
    try:
        for line in open(path):
            k, _, v = line.strip().partition("=")
            d[k] = v
    except OSError:
        pass
    return d


def num(d, k, default=0):
    try:
        return float(d.get(k, default))
    except ValueError:
        return default


def read1(path, default=""):
    try:
        return open(path).read().strip()
    except OSError:
        return default


def client(path, t_usr1, t_a_gone, t_reg, t_kick):
    rows = []
    for line in open(path) if os.path.exists(path) else []:
        f = dict(kv.split("=", 1) for kv in line.split())
        rows.append({k: float(v) for k, v in f.items()})
    if not rows:
        return {"lines": 0}

    def ok_at(t):
        prev = rows[0]
        for r in rows:
            if r["t"] > t:
                break
            prev = r
        return prev

    pre = ok_at(t_usr1)
    last = rows[-1]
    # stall episodes after SIGUSR1: a read/open in flight for >= 300 ms.
    # [start, end] relative to A being gone; end None = still stuck at the end
    stalls, cur = [], None
    for r in rows:
        if r["t"] < t_usr1:
            continue
        infl = r["inflight_ms"]
        if infl >= 300:
            st = r["t"] - infl / 1000
            if cur is None or abs(cur[0] - st) > 0.05:
                if cur is not None:
                    stalls.append(cur)
                cur = [st, None]
        elif cur is not None:
            cur[1] = r["t"]
            stalls.append(cur)
            cur = None
    if cur is not None:
        stalls.append(cur)
    rel = [[round(a - t_a_gone, 2), None if b is None else round(b - t_a_gone, 2)] for a, b in stalls]
    return {
        "ok_before": int(pre["ok"]),
        "ok_after_usr1": int(last["ok"] - pre["ok"]),
        "err_after": int(last["err"] - pre["err"]),
        "bad": int(last["bad"]),
        "last_err": ERRNO.get(int(last["last_err"]), str(int(last["last_err"]))),
        "stalls": rel,
        "lost": last["inflight_ms"] >= 3000,
        "stuck_at_end_ms": int(last["inflight_ms"]),
    }


def analyze(d):
    a_pre = stats(f"{d}/a.stats.prehandover")
    a_fin = stats(f"{d}/a.stats.final")
    b = stats(f"{d}/b.stats.final")
    b_kick = stats(f"{d}/b.stats.before_kick")
    t_usr1 = float(read1(f"{d}/t_usr1", "0") or 0)
    t_kick = float(read1(f"{d}/t_kick", "0") or 0)
    t_a_gone = num(b, "t_a_gone") or float(read1(f"{d}/t_a_exit", "0") or 0)
    t_reg = num(b, "t_registered")
    probes = {}
    for p in sorted(glob.glob(f"{d}/probe.cpu*"), key=lambda s: int(s.rsplit("cpu", 1)[1])):
        txt = read1(p)
        cpu = int(p.rsplit("cpu", 1)[1])
        if "HUNG" in txt:
            m = re.search(r"HUNG (\w+)", txt)
            probes[cpu] = "hung-" + (m.group(1) if m else "?")
        elif txt.startswith("ok"):
            probes[cpu] = "ok"
        else:
            m = re.search(r"err=(\d+)", txt)
            probes[cpu] = "err-" + ERRNO.get(int(m.group(1)), m.group(1)) if m else txt[:30]
    gp = read1(f"{d}/probe.gap")
    t_gp = float(read1(f"{d}/t_gapprobe", "0") or 0)
    m = re.search(r"ms=(\d+)", gp)
    if "HUNG" in gp or not gp:
        gap_probe = {"result": "hung", "done_s_after_a_gone": None}
    else:
        done = t_gp + int(m.group(1)) / 1000 if m else None
        gap_probe = {
            "result": "ok" if gp.startswith("ok") else gp[:40],
            "done_s_after_a_gone": round(done - t_a_gone, 2) if done else None,
            "done_vs_register": round(done - t_reg, 2) if done and t_reg else None,
            "done_vs_kick": round(done - t_kick, 2) if done else None,
        }
    fin = read1(f"{d}/final.state")
    dm = [l for l in read1(f"{d}/dmesg.txt").splitlines() if l.strip()]
    kernel = os.path.basename(os.path.dirname(os.path.abspath(d)))
    m = re.match(r"v(\d+)-r(\d+)", os.path.basename(d))
    r = {
        "kernel": kernel,
        "variant": int(m.group(1)),
        "repeat": int(m.group(2)),
        "failed": read1(f"{d}/FAILED") or None,
        "a_ring_reqs_before": int(num(a_pre, "ring_reqs")),
        "a_dev_reqs_before": int(num(a_pre, "dev_reqs")),
        "a_ring_reqs_total": int(num(a_fin, "ring_reqs")),
        "a_final_phase": a_fin.get("phase"),
        "b_phase": b.get("phase"),
        "b_dev_reqs": int(num(b, "dev_reqs")),
        "b_dev_total": int(num(b, "dev_total")),
        "b_ring_reqs_before_kick": int(num(b_kick, "ring_reqs")),
        "b_ring_reqs": int(num(b, "ring_reqs")),
        "b_reg_submitted": int(num(b, "reg_submitted")),
        "b_reg_rejected": int(num(b, "reg_err_count")),
        "b_reg_first_err": int(num(b, "reg_first_err")),
        "b_cqe_err": int(num(b, "cqe_err_count")),
        "b_cqe_first_err": int(num(b, "cqe_first_err")),
        "a_gone_s_after_usr1": round(t_a_gone - t_usr1, 2) if t_a_gone else None,
        "register_s_after_a_gone": round(t_reg - t_a_gone, 2) if t_reg and t_a_gone else None,
        "kick_s_after_a_gone": round(t_kick - t_a_gone, 2) if t_a_gone else None,
        "client1": client(f"{d}/client1.log", t_usr1, t_a_gone, t_reg, t_kick),
        "client5": client(f"{d}/client5.log", t_usr1, t_a_gone, t_reg, t_kick),
        "probes": probes,
        "gap_probe": gap_probe,
        "final_state": fin.replace("\n", " | "),
        "aborted": read1(f"{d}/aborted") == "1",
        "waiting": {k: read1(f"{d}/waiting.{k}") for k in
                    ("start", "after_a", "before_kick", "final")},
        "dmesg": dm[:20],
    }
    return r


def human(r):
    def c(x):
        cl = r[x]
        if not cl.get("lines", 1):
            return f"{x}: no log"
        return (f"{x}: pre={cl['ok_before']} post={cl['ok_after_usr1']} "
                f"err={cl['err_after']}({cl['last_err']}) bad={cl['bad']} "
                f"stalls={cl['stalls']} {'LOST' if cl['lost'] else 'alive'}")
    pr = " ".join(f"{k}:{v}" for k, v in r["probes"].items())
    gp = r.get("gap_probe", {})
    pr += f" | gap-probe {gp.get('result')} reg{gp.get('done_vs_register')} kick{gp.get('done_vs_kick')}"
    return (f"[{r['kernel']} v{r['variant']} r{r['repeat']}] "
            f"A ring={r['a_ring_reqs_before']} dev={r['a_dev_reqs_before']} | "
            f"B dev={r['b_dev_reqs']} ring={r['b_ring_reqs_before_kick']}->{r['b_ring_reqs']} "
            f"reg={r['b_reg_submitted']}-{r['b_reg_rejected']}rej({r['b_reg_first_err']}) "
            f"cqe_err={r['b_cqe_err']}({r['b_cqe_first_err']}) | {c('client1')} | {c('client5')} | "
            f"kick {pr} | waiting {r['waiting']} | aborted={r['aborted']} | {r['final_state']} | "
            f"dmesg {len(r['dmesg'])} lines")


def matrix(files):
    runs = [json.loads(l) for f in files for l in open(f)]
    kernels = sorted({r["kernel"] for r in runs})
    out = ["| kernel | variant | runs | (a) connection alive | (b) B served over /dev/fuse | "
           "(c) B's REGISTERs accepted / B served over ring | client reads lost at handover | "
           "gap probe done (s after B's REGISTER / after kick) | kick probes (8 CPUs) | "
           "fusectl abort needed | dmesg lines |",
           "|---|---|---|---|---|---|---|---|---|---|---|"]
    for k in kernels:
        for v in (1, 2, 3, 4):
            rs = [r for r in runs if r["kernel"] == k and r["variant"] == v]
            if not rs:
                continue
            n = len(rs)
            alive = sum(1 for r in rs if not any(
                "ENOTCONN" in str(p) or "ECONNABORTED" in str(p) for p in r["probes"].values())
                and r["client1"]["err_after"] + r["client5"]["err_after"] == 0)
            devs = sum(r["b_dev_reqs"] for r in rs)
            reg = sum(r["b_reg_submitted"] for r in rs)
            rej = sum(r["b_reg_rejected"] for r in rs)
            ring = [r["b_ring_reqs"] for r in rs]
            lost = [int(r["client1"]["lost"]) + int(r["client5"]["lost"]) for r in rs]
            gps = [r["gap_probe"] for r in rs]
            gp_ok = [g for g in gps if g["result"] == "ok"]
            if not gp_ok:
                gp = f"hung ({n}/{n})"
            else:
                regs = sorted({g["done_vs_register"] for g in gp_ok if g.get("done_vs_register") is not None})
                kicks = sorted({g["done_vs_kick"] for g in gp_ok})
                gp = f"ok {len(gp_ok)}/{n}: +{regs[0]}..{regs[-1]} / {kicks[0]}..{kicks[-1]}" if regs \
                    else f"ok {len(gp_ok)}/{n}"
            kick_ok = sum(1 for r in rs for p in r["probes"].values() if p == "ok")
            kick_n = sum(len(r["probes"]) for r in rs)
            hung = sum(1 for r in rs for p in r["probes"].values() if p.startswith("hung"))
            kick = f"{kick_ok}/{kick_n} ok" + (f", {hung} hung (killable)" if hung else "")
            ab = sum(1 for r in rs if r["aborted"])
            dm = sum(len(r["dmesg"]) for r in rs)
            out.append(f"| {k} | {v} | {n} | {alive}/{n} | {devs} requests | "
                       f"{'n/a (never registers)' if not reg else f'{reg - rej}/{reg}, {min(ring)}..{max(ring)} requests/run'} | "
                       f"{sum(lost)} of {2 * n} ({'/'.join(map(str, lost))}) | {gp} | {kick} | {ab}/{n} | {dm} |")
    return "\n".join(out)


if __name__ == "__main__":
    if sys.argv[1:] == ["--human"]:
        for line in sys.stdin:
            print(human(json.loads(line)))
    elif sys.argv[1:2] == ["--matrix"]:
        print(matrix(sys.argv[2:]))
    elif sys.argv[1:2] == ["--table"]:
        for f in sys.argv[2:]:
            for line in open(f):
                print(human(json.loads(line)))
    else:
        print(json.dumps(analyze(sys.argv[1])))
