#!/usr/bin/env python3
"""N empty files in one directory. usage: mkbig.py DIR N"""
import os, sys
d, n = sys.argv[1], int(sys.argv[2])
os.makedirs(d, exist_ok=True)
for i in range(n):
    open(f"{d}/e{i:07d}-xxxxxxxxxxxx", "w").close()
