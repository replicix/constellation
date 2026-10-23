#!/usr/bin/env python3
"""FUSE request counts and the plan 42 counters from a running daemon.
  snap.py BIN STATE_DIR          print `node.status` as a compact JSON (ops by name, kernel counters)
  snap.py --diff A.json B.json   key=value pairs: requests (total), read/write/getattr/lookup/open/... deltas,
                                 daemon service time (ms), and the kernel-cache counters' deltas
  snap.py --fuse B.json          the kernel-cache counters as of B, key=value pairs (final build only)"""
import json, os, subprocess, sys

OPS = ("read", "write", "getattr", "lookup", "open", "release", "create", "mkdir", "unlink", "rename",
       "readdir", "readdirplus", "setattr", "flush", "fsync", "forget")

def snap(bin_, state):
    env = dict(os.environ)
    out = subprocess.run([bin_, "status", "/", "--state-dir", state], capture_output=True, text=True, timeout=60, env=env)
    d = json.loads(out.stdout)
    ops, ns = {}, 0
    for s in d["vfs_ops"]["series"]:
        if s["frontend"] != "fuse":
            continue
        n = sum(s["outcomes"].values())
        ops[s["op"]] = ops.get(s["op"], 0) + n
        ns += s["sum_ns"]
    kc = {}
    for m in d["fuse"]["mounts"]:
        for k, v in (m.get("kernel_cache") or {}).items():
            if isinstance(v, dict):
                for k2, v2 in v.items():
                    kc[f"{k}.{k2}"] = kc.get(f"{k}.{k2}", 0) + v2
            elif isinstance(v, (int, float)):
                kc[k] = kc.get(k, 0) + v
    return {"ops": ops, "service_ns": ns, "kc": kc}

def diff(a, b):
    out = []
    ta, tb = sum(a["ops"].values()), sum(b["ops"].values())
    out.append(f"reqs={tb - ta}")
    for op in sorted(set(a["ops"]) | set(b["ops"])):
        dl = b["ops"].get(op, 0) - a["ops"].get(op, 0)
        if dl and op in OPS:
            out.append(f"n_{op}={dl}")
    out.append(f"service_ms={(b['service_ns'] - a['service_ns']) / 1e6:.0f}")
    for k in sorted(set(a["kc"]) | set(b["kc"])):
        dl = b["kc"].get(k, 0) - a["kc"].get(k, 0)
        if dl:
            out.append(f"kc_{k}={dl}")
    return " ".join(out)

if sys.argv[1] == "--diff":
    print(diff(json.load(open(sys.argv[2])), json.load(open(sys.argv[3]))))
elif sys.argv[1] == "--fuse":
    b = json.load(open(sys.argv[2]))
    print(" ".join(f"kcT_{k}={v}" for k, v in sorted(b["kc"].items()) if v))
else:
    print(json.dumps(snap(sys.argv[1], sys.argv[2])))
