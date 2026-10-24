#!/usr/bin/env python3
"""Appendix A.1's tree: 20 x 10 x 10 directories of 25 files (24 sources and a header),
a git repository of it (packed), a Makefile with 5,000 up-to-date targets, each
depending on a source and a header. usage: gen-tree.py DIR   (DIR must not exist)"""
import os, subprocess, sys, time

root = sys.argv[1]
os.makedirs(root)
src_base = 1_700_000_000
cand = []
for a in range(20):
    for b in range(10):
        for c in range(10):
            d = f"{root}/a{a:02d}/b{b}/c{c}"
            os.makedirs(d)
            with open(f"{d}/common.h", "w") as f:
                f.write(f"#define LEAF_{a}_{b}_{c} 1\nint leaf_{a}_{b}_{c}(int);\n")
            for k in range(24):
                with open(f"{d}/f{k:02d}.c", "w") as f:
                    f.write(f'#include "common.h"\nint f_{a}_{b}_{c}_{k}(int x) {{ return x + {k}; }}\n')
                if k < 3:
                    cand.append((f"a{a:02d}/b{b}/c{c}/f{k:02d}.c", f"a{a:02d}/b{b}/c{c}/common.h", k))
targets = [(s, h) for s, h, k in sorted(cand, key=lambda x: x[2])][:5000]
os.makedirs(f"{root}/obj")
with open(f"{root}/Makefile", "w") as f:
    f.write("all: " + " ".join(f"obj/o{i:04d}.o" for i in range(len(targets))) + "\n")
    for i, (s, h) in enumerate(targets):
        f.write(f"obj/o{i:04d}.o: {s} {h}\n\ttouch $@\n")
# fixed mtimes: sources at T, objects at T+100 (whole seconds, so a tar keeps them)
for dp, dn, fn in os.walk(root):
    for x in fn:
        os.utime(os.path.join(dp, x), (src_base, src_base))
for i in range(len(targets)):
    p = f"{root}/obj/o{i:04d}.o"
    open(p, "w").close()
    os.utime(p, (src_base + 100, src_base + 100))
env = dict(os.environ, GIT_AUTHOR_NAME="b", GIT_AUTHOR_EMAIL="b@x", GIT_COMMITTER_NAME="b",
           GIT_COMMITTER_EMAIL="b@x", GIT_CONFIG_GLOBAL="/dev/null")
def git(*a):
    subprocess.run(["git", "-c", "gc.auto=0", "-C", root, *a], check=True, env=env, stdout=subprocess.DEVNULL)
git("init", "-q")
git("add", "-A")
git("commit", "-q", "-m", "tree")
git("gc", "-q")
print("targets", len(targets))
