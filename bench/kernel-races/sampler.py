#!/usr/bin/env python3
"""Every 5 s: the 1-minute load average, runnable tasks, the CPU cores busy in all (from /proc/stat) and the cores
used by the processes of this session (the benchmark's own); the difference is what the other work on the host used.
usage: sampler.py OUTFILE SESSION_ID"""
import os, sys, time

out, sid = sys.argv[1], sys.argv[2]
clk = os.sysconf("SC_CLK_TCK")
ncpu = os.cpu_count()

def total():
    f = open("/proc/stat").readline().split()[1:]
    v = [int(x) for x in f]
    idle = v[3] + v[4]
    return sum(v), idle

def mine():
    s = 0
    for p in os.listdir("/proc"):
        if not p.isdigit():
            continue
        try:
            st = open(f"/proc/{p}/stat").read()
            r = st[st.rindex(")") + 2:].split()
            if r[3] != sid:
                continue
            s += int(r[11]) + int(r[12])
        except (OSError, ValueError, IndexError):
            continue
    return s

t0, i0 = total(); m0 = mine(); w0 = time.time()
with open(out, "a", buffering=1) as f:
    while True:
        time.sleep(5)
        t1, i1 = total(); m1 = mine(); w1 = time.time()
        dt = max(t1 - t0, 1)
        busy = (dt - (i1 - i0)) / dt * ncpu
        mycores = max(m1 - m0, 0) / clk / (w1 - w0)
        la = open("/proc/loadavg").read().split()
        f.write(f"S {int(w1)} load1={la[0]} running={la[3].split('/')[0]} busy={busy:.2f} mine={mycores:.2f}\n")
        t0, i0, m0, w0 = t1, i1, m1, w1
