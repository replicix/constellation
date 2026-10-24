#!/usr/bin/env python3
"""Directory-stream helper for cell.py (libc opendir/readdir/telldir/seekdir through ctypes).

  listtool.py hold DIR N          opendir, read N entries, print READY, keep the stream open until stdin closes
  listtool.py openclose DIR N     N times opendir + one readdir + closedir; print the per-call latencies
  listtool.py coldresume DIR N    read N entries, remember the position, reopen (no hint), seekdir, time one page
"""
import ctypes
import ctypes.util
import sys
import time

libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
libc.opendir.restype = ctypes.c_void_p
libc.opendir.argtypes = [ctypes.c_char_p]
libc.readdir64.restype = ctypes.c_void_p
libc.readdir64.argtypes = [ctypes.c_void_p]
libc.telldir.restype = ctypes.c_long
libc.telldir.argtypes = [ctypes.c_void_p]
libc.seekdir.argtypes = [ctypes.c_void_p, ctypes.c_long]
libc.closedir.argtypes = [ctypes.c_void_p]


def opendir(p):
    d = libc.opendir(p.encode())
    if not d:
        raise OSError(ctypes.get_errno(), "opendir")
    return d


def take(d, n):
    got = 0
    while got < n:
        if not libc.readdir64(d):
            break
        got += 1
    return got


def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(len(v) * p))]


mode, path = sys.argv[1], sys.argv[2]
if mode == "hold":
    d = opendir(path)
    take(d, int(sys.argv[3]))
    print("READY", flush=True)
    sys.stdin.read()
    libc.closedir(d)
elif mode == "openclose":
    lat = []
    for _ in range(int(sys.argv[3])):
        d = opendir(path)
        take(d, 1)
        t = time.perf_counter()
        libc.closedir(d)
        lat.append(time.perf_counter() - t)
    print(f"closedir_us p50={pct(lat, .5) * 1e6:.0f} p99={pct(lat, .99) * 1e6:.0f} max={max(lat) * 1e6:.0f} n={len(lat)}")
elif mode == "coldresume":
    d = opendir(path)
    take(d, int(sys.argv[3]))
    pos = libc.telldir(d)
    libc.closedir(d)
    d = opendir(path)
    t = time.perf_counter()
    libc.seekdir(d, pos)
    got = take(d, 1)
    first = time.perf_counter() - t
    t = time.perf_counter()
    got += take(d, 500)
    page = time.perf_counter() - t
    print(f"cold_resume_first_entry_ms={first * 1e3:.1f} next500_ms={page * 1e3:.1f} entries={got}")
