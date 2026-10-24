#!/usr/bin/env python3
"""2,000 directories of 100 small files (200,000 files, sizes 0-2 KiB), fixed mtimes. usage: gentree200k.py DIR"""
import os, sys, random
root = sys.argv[1]
rng = random.Random(40)
os.makedirs(root, exist_ok=True)
for d in range(2000):
    p = f"{root}/d{d:04d}"
    os.makedirs(p, exist_ok=True)
    for f in range(100):
        fp = f"{p}/file-{d:04d}-{f:03d}.dat"
        with open(fp, "wb") as fh:
            fh.write(os.urandom(rng.randrange(0, 2048)))
        os.utime(fp, (1_700_000_000 + f, 1_700_000_000 + f))
print("files", 200000)
