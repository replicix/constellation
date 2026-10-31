#!/usr/bin/env python3
"""Plan 40 section 8: one benchmark cell -- one build, one backing store, one READDIRPLUS mode.

  cell.py BIN STOREDIR TAG OUT [--mode off|auto|always|none] [--phases list,write,big,handover]

  BIN       constellation binary (the base build or the build under test)
  STOREDIR  directory on the file system the state dir and the file store live on (ext4, xfs, btrfs, tmpfs):
            everything of the cell goes in STOREDIR/p40-8-TAG and is removed at the end
  OUT       result file: one JSON object per line (kind "cell", "run", "result", "load")
  --mode    CONSTELLATION_FUSE_READDIRPLUS for the build under test; `none` sets nothing (the base build has no knob)

Environment (all optional):
  SIZES       entries of the listed directories, default "1000 10000 100000"
  BIG_SIZES   directories for the `big` phase (listing and memory only), default "1000000"; "1000000 10000000" is the
              plan's (the 10M directory takes about an hour to create at 3k files/s)
  REPS        repetitions per measurement, default 7 (the plan's median of 7)
  TREE        tree.tar of bench/readdirplus/gen-tree.py (the 50k-file, 2,000-directory tree); without it the tree
              workloads are skipped
  LOAD_MAX    1-minute load gate before each phase, default 4 (the shared hosts' rule for benchmarks)
  PHASE_WAIT  seconds a gate waits before it records `gate_ok=false` and goes on, default 900
  STORM       entries of the create/unlink/rename storm directory, default 100000
  TREE2       tree.tar for the `trees` phase (plan 42's tree workloads, cold and warm; TREE_REPS repetitions)
  BIG_LISTS_ONLY  set: the `big` phase measures the listings and the cold resume only (no pins, no handover)
  WRITE_MODES write modes of the `write` phase, default "through back"

Workloads are the plan's (`ls -f`, `ls`, `find -name`, `ls -l`, `ls -la`, `find -ls`, `find -newer`, `find -size`,
`du -s`, `rsync -a --dry-run`, a second `ls -l` within the TTL, and an `xargs stat` storm as a regression check).
A kernel-cold run is a fresh mount of a warm replica: the daemon is restarted for every repetition. Each run records
the wall time, the daemon's CPU (utime + stime from /proc/PID/stat), its peak RSS (VmHWM, reset before the run), the
FUSE requests by op (node.status `vfs_ops`), and the mount's READDIRPLUS counters.
"""
import argparse
import ctypes
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
TICK = os.sysconf("SC_CLK_TCK")


def load1():
    return float(open("/proc/loadavg").read().split()[0])


class Cell:
    def __init__(self, a):
        self.bin = os.path.abspath(a.bin)
        self.tag = a.tag
        self.root = os.path.join(os.path.abspath(a.store), "p40-8-" + a.tag)
        self.out = open(a.out, "a", buffering=1)
        # one mode, or a list ("off,auto"): a pair cell alternates the modes within one populated state dir, A B then
        # B A on the next repetition (ABBA), which cancels the host's drift at the grain of one run
        self.modes = a.mode.split(",")
        self.mode = self.modes[0]
        self.mnt = os.path.join(self.root, "mnt")
        self.state = os.path.join(self.root, "state")
        self.backend = os.path.join(self.root, "backend")
        self.home = os.path.join(self.root, "home")
        self.log = os.path.join(self.root, "mount.log")
        self.proc = None
        self.mounted_mode = None
        self.load_max = float(os.environ.get("LOAD_MAX", "4"))
        self.phase_wait = int(os.environ.get("PHASE_WAIT", "900"))
        self.reps = int(os.environ.get("REPS", "7"))
        self.write_mode = None
        self.rng = random.Random(40)

    # ------------------------------------------------------------ records
    def emit(self, kind, **kw):
        kw.setdefault("mode", self.mode)
        kw.update(kind=kind, tag=self.tag, t=int(time.time()))
        self.out.write(json.dumps(kw, sort_keys=True) + "\n")

    def gate(self, name):
        t0 = time.time()
        ok = True
        while load1() >= self.load_max:
            if time.time() - t0 > self.phase_wait:
                ok = False
                break
            time.sleep(15)
        self.emit("load", phase=name, load1=load1(), waited=int(time.time() - t0), gate_ok=ok)

    # ------------------------------------------------------------- daemon
    def env(self, extra=None):
        e = dict(os.environ, HOME=self.home)
        if self.mode != "none":
            e["CONSTELLATION_FUSE_READDIRPLUS"] = self.mode
        e.update(extra or {})
        return e

    def create_fs(self):
        subprocess.run(["rm", "-rf", self.root])
        for d in (self.home, self.state, self.backend, self.mnt):
            os.makedirs(d)
        r = subprocess.run([self.bin, "fs", "create", "bench", "--s3", "file://" + self.backend,
                            "--chunk-size", "1048576"], env=self.env(), capture_output=True, text=True)
        if r.returncode:
            raise SystemExit("fs create failed: " + r.stderr)

    def mount(self, extra=None, args=()):
        cmd = [self.bin, "mount", "/", self.mnt, "--s3", "file://" + self.backend, "--state-dir", self.state,
               "--cache-size", "4G", "--foreground"]
        if self.write_mode:
            cmd += ["--write-mode", self.write_mode]
        cmd += list(args)
        self.mounted_mode = self.mode
        self.proc = subprocess.Popen(["timeout", "--kill-after=10s", "7200"] + cmd, env=self.env(extra),
                                     stdout=open(self.log, "ab"), stderr=subprocess.STDOUT)
        for _ in range(240):
            if os.path.ismount(self.mnt):
                break
            time.sleep(0.5)
        else:
            raise SystemExit("mount failed: " + open(self.log).read()[-2000:])
        # `timeout` is the child; the daemon is its child.
        self.pid = int(subprocess.check_output(["pgrep", "-P", str(self.proc.pid)]).split()[0])

    def umount(self):
        subprocess.run(["fusermount3", "-u", self.mnt], capture_output=True)
        if self.proc:
            try:
                self.proc.wait(timeout=90)
            except subprocess.TimeoutExpired:
                self.proc.terminate()
                self.proc.wait(timeout=30)
        self.proc = None

    def remount(self, extra=None):
        self.umount()
        self.mount(extra)

    def status(self):
        r = subprocess.run([self.bin, "status", "/", "--state-dir", self.state], env=self.env(),
                           capture_output=True, text=True, timeout=60)
        return json.loads(r.stdout)

    def snap(self):
        st = self.status()
        ops = {}
        ns = {}
        for s in st["vfs_ops"]["series"]:
            if s["frontend"] == "fuse":
                ops[s["op"]] = ops.get(s["op"], 0) + sum(s["outcomes"].values())
                ns[s["op"]] = ns.get(s["op"], 0) + s.get("sum_ns", 0)
        m = (st["fuse"]["mounts"] or [{}])[0]
        keep = ("kernel_refs", "readdirplus", "readdirplus_pages", "readdirplus_entries", "readdirplus_name_only",
                "unlinked_refs")
        return {"ops": ops, "ns": ns, "mount": {k: m.get(k) for k in keep}, "readdir": st.get("readdir", {})}

    def cpu(self):
        f = open(f"/proc/{self.pid}/stat").read()
        r = f[f.rindex(")") + 2:].split()
        return (int(r[11]) + int(r[12])) / TICK

    def rss_reset(self):
        try:
            open(f"/proc/{self.pid}/clear_refs", "w").write("5")
        except OSError:
            pass

    def rss_peak_kb(self):
        for line in open(f"/proc/{self.pid}/status"):
            if line.startswith("VmHWM:"):
                return int(line.split()[1])
        return 0

    # ------------------------------------------------------------ measure
    def rep_modes(self, reps):
        """(rep, mode) pairs: the modes in order on even repetitions, reversed on odd ones."""
        for rep in range(reps):
            for m in (self.modes if rep % 2 == 0 else self.modes[::-1]):
                yield rep, m

    def measure(self, name, cmd, size, cold=True, warm_first=None, shell=False, extra=None):
        """REPS runs of `cmd`: a fresh mount per run when `cold`; `warm_first` runs once before the timed run."""
        for rep, mode in self.rep_modes(self.reps):
            self.mode = mode
            if cold or self.mounted_mode != mode:
                self.remount()
            if warm_first:
                subprocess.run(warm_first, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=shell)
            self.rss_reset()
            s0, c0 = self.snap(), self.cpu()
            t0 = time.time()
            r = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=shell)
            wall = time.time() - t0
            c1, s1 = self.cpu(), self.snap()
            reqs = {k: s1["ops"].get(k, 0) - s0["ops"].get(k, 0) for k in set(s1["ops"]) | set(s0["ops"])}
            reqs = {k: v for k, v in reqs.items() if v}
            nsd = {k: s1["ns"].get(k, 0) - s0["ns"].get(k, 0) for k in set(s1["ns"]) | set(s0["ns"])}
            nsd = {k: v for k, v in nsd.items() if v}
            self.emit("run", name=name, size=size, rep=rep, wall=round(wall, 4), rc=r.returncode,
                      daemon_cpu=round(c1 - c0, 3), rss_peak_kb=self.rss_peak_kb(), reqs=reqs, ns_by_op=nsd,
                      requests=sum(reqs.values()), mount=s1["mount"],
                      plus_entries=(s1["mount"].get("readdirplus_entries") or 0) - (s0["mount"].get("readdirplus_entries") or 0),
                      readdir=s1["readdir"])

    # ------------------------------------------------------------- trees
    def names(self, n):
        """n unique names of 12-40 bytes: an index prefix, then filler."""
        rng = random.Random(n)
        filler = "".join(rng.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=40))
        return [(f"{i:08d}-" + filler)[:rng.randint(12, 40)] for i in range(n)]

    def fill(self, path, names, sizes=None, threads=16):
        os.makedirs(path, exist_ok=True)
        nxt = [0]
        lock = threading.Lock()

        def work():
            while True:
                with lock:
                    i = nxt[0]
                    nxt[0] += 1
                if i >= len(names):
                    return
                with open(os.path.join(path, names[i]), "wb") as f:
                    if sizes:
                        f.truncate(sizes[i])  # sparse: the size without the bytes
        ts = [threading.Thread(target=work) for _ in range(threads)]
        [t.start() for t in ts]
        [t.join() for t in ts]

    def make_dir(self, path, n):
        rng = random.Random(n + 1)
        self.fill(path, self.names(n), [rng.randint(0, 64 * 1024) for _ in range(n)])

    # ---------------------------------------------------------- the phases
    def phase_list(self):
        sizes = [int(x) for x in os.environ.get("SIZES", "1000 10000 100000").split()]
        self.gate("populate")
        self.mount()
        for n in sizes:
            t0 = time.time()
            self.make_dir(os.path.join(self.mnt, f"t{n}"), n)
            self.emit("result", name="populate", size=n, wall=round(time.time() - t0, 2))
        tree = os.environ.get("TREE")
        if tree:
            os.makedirs(os.path.join(self.mnt, "tree"))
            subprocess.run(["tar", "-xf", tree, "--strip-components=1", "-C", os.path.join(self.mnt, "tree")], check=True)
        subprocess.run(["sync"])
        self.umount()
        names_dir = os.path.join(self.root, "names")
        os.makedirs(names_dir)
        empty = os.path.join(self.root, "empty")
        for n in sizes:
            self.gate(f"list-{n}")
            d = os.path.join(self.mnt, f"t{n}")
            nf = os.path.join(names_dir, f"{n}.txt")
            self.mount()
            with open(nf, "w") as f:
                for e in os.listdir(d):
                    f.write(os.path.join(d, e) + "\n")
            workloads = [
                ("ls_f", ["ls", "-f", d]),
                ("ls", ["ls", d]),
                ("find_name", ["find", d, "-name", "nomatch"]),
                ("ls_l", ["ls", "-l", d]),
                ("ls_la", ["ls", "-la", d]),
                ("find_ls", ["find", d, "-ls"]),
                ("find_newer", ["find", d, "-newer", "/etc/hostname"]),
                ("find_size", ["find", d, "-size", "+1k"]),
                ("du_s", ["du", "-s", d]),
                ("rsync_n", ["rsync", "-a", "--dry-run", d + "/", empty + "/"]),
                ("xargs_stat", ["sh", "-c", f"xargs -a '{nf}' -n 1000 stat > /dev/null"]),
            ]
            os.makedirs(empty, exist_ok=True)
            for name, cmd in workloads:
                self.measure(name, cmd, n)
            # a second ls -l right after the first: the stat storm with the kernel cache warm
            self.measure("ls_l_2nd", ["ls", "-l", d], n, warm_first=["ls", "-l", d])
            self.umount()
        if tree:
            self.gate("tree")
            t = os.path.join(self.mnt, "tree")
            self.mount()
            for name, cmd in (("tree_ls_lR", ["ls", "-lR", t]), ("tree_find_ls", ["find", t, "-ls"]),
                              ("tree_du", ["du", "-s", t]),
                              ("tree_git_status", ["git", "-C", t, "status", "--porcelain"])):
                self.measure(name, cmd, 50000)
            self.umount()

    def phase_trees(self):
        """The tree workloads of plan 42 section 8 (bench/kernel-races/cell.sh), kernel-cold and warm, per mode.
        t1 is the 50k-file tree (gen-tree.py, with its git repository and 5,000-target Makefile), t2 a copy of it
        for `rsync -an --delete t1/ t2/`. Each repetition: fresh mount (the host's page cache is not dropped: replica warm), the cold run, then the same
        command again at once (warm: the replica and the kernel's caches hold what the first run read)."""
        tree = os.environ["TREE2"]
        self.gate("trees-populate")
        self.write_mode = "back"  # the populate only; the measured mounts use the default mode
        self.create_fs()
        self.mount()
        t1, t2 = os.path.join(self.mnt, "t1"), os.path.join(self.mnt, "t2")
        os.makedirs(t1)
        subprocess.run(["tar", "-xf", tree, "--strip-components=1", "-C", t1], check=True)
        subprocess.run(["cp", "-a", t1, t2], check=True)
        env = dict(os.environ, GIT_CONFIG_GLOBAL="/dev/null")
        subprocess.run(["git", "-C", t1, "update-index", "-q", "--refresh"], env=env, stderr=subprocess.DEVNULL)
        subprocess.run(["git", "-C", t1, "status", "--porcelain"], env=env, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
        subprocess.run(["sync"])
        self.umount()
        self.write_mode = None
        self.gate("trees")
        work = [("ls_lR", ["ls", "-lR", t1]), ("find_f", ["find", t1, "-type", "f"]),
                ("git_status", ["git", "-C", t1, "status", "--porcelain"]),
                ("make_q", ["make", "-q", "-C", t1, "all"]),
                ("rsync_n", ["rsync", "-an", "--delete", t1 + "/", t2 + "/"])]
        reps = int(os.environ.get("TREE_REPS", self.reps))
        for name, cmd in work:
            for rep, mode in self.rep_modes(reps):
                self.mode = mode
                self.remount()
                for state in ("cold", "warm"):
                    self.rss_reset()
                    s0, c0 = self.snap(), self.cpu()
                    t0 = time.time()
                    r = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
                    wall = time.time() - t0
                    c1, s1 = self.cpu(), self.snap()
                    reqs = {k: s1["ops"].get(k, 0) - s0["ops"].get(k, 0) for k in set(s1["ops"]) | set(s0["ops"])}
                    reqs = {k: v for k, v in reqs.items() if v}
                    nsd = {k: s1["ns"].get(k, 0) - s0["ns"].get(k, 0) for k in set(s1["ns"]) | set(s0["ns"])}
                    nsd = {k: v for k, v in nsd.items() if v}
                    self.emit("run", name=f"tree_{name}_{state}", size=50000, rep=rep, wall=round(wall, 4),
                              rc=r.returncode, daemon_cpu=round(c1 - c0, 3), reqs=reqs, ns_by_op=nsd,
                              requests=sum(reqs.values()),
                              plus_entries=(s1["mount"].get("readdirplus_entries") or 0) - (s0["mount"].get("readdirplus_entries") or 0),
                              name_only=(s1["mount"].get("readdirplus_name_only") or 0) - (s0["mount"].get("readdirplus_name_only") or 0))
        self.umount()

    def phase_write(self):
        modes = os.environ.get("WRITE_MODES", "through back").split()
        storm = int(os.environ.get("STORM", "100000"))
        tree = os.environ.get("TREE")
        for wm in modes:
            self.write_mode = wm
            self.gate(f"write-{wm}")
            self.create_fs()
            self.mount()
            d = os.path.join(self.mnt, "storm")
            os.makedirs(d)
            res = {}
            for what, fn in (("create", lambda i: open(os.path.join(d, f"e{i:07d}"), "w").close()),
                             ("rename", lambda i: os.rename(os.path.join(d, f"e{i:07d}"), os.path.join(d, f"r{i:07d}"))),
                             ("unlink", lambda i: os.unlink(os.path.join(d, f"r{i:07d}")))):
                s0, c0, t0 = self.snap(), self.cpu(), time.time()
                for i in range(storm):
                    fn(i)
                wall = time.time() - t0
                s1 = self.snap()
                reqs = {k: s1["ops"].get(k, 0) - s0["ops"].get(k, 0) for k in s1["ops"]}
                self.emit("run", name=f"storm_{what}", size=storm, rep=0, wall=round(wall, 3), write_mode=wm,
                          daemon_cpu=round(self.cpu() - c0, 3), requests=sum(v for v in reqs.values() if v),
                          reqs={k: v for k, v in reqs.items() if v})
            if tree:
                for rep in range(max(1, self.reps // 2)):
                    t = os.path.join(self.mnt, f"untar{rep}")
                    os.makedirs(t)
                    s0, c0, t0 = self.snap(), self.cpu(), time.time()
                    subprocess.run(["tar", "-xf", tree, "--strip-components=1", "-C", t], check=True)
                    wall = time.time() - t0
                    s1 = self.snap()
                    reqs = {k: s1["ops"].get(k, 0) - s0["ops"].get(k, 0) for k in s1["ops"]}
                    self.emit("run", name="untar", size=50000, rep=rep, wall=round(wall, 3), write_mode=wm,
                              daemon_cpu=round(self.cpu() - c0, 3), requests=sum(v for v in reqs.values() if v),
                              reqs={k: v for k, v in reqs.items() if v})
            self.umount()
        self.write_mode = None

    def phase_big(self):
        """Listing and memory of the very large directories; pins; cold resume; releasedir."""
        lt = os.path.join(HERE, "listtool.py")
        for n in [int(x) for x in os.environ.get("BIG_SIZES", "1000000").split()]:
            self.gate(f"big-{n}")
            self.write_mode = "back"
            self.create_fs()
            self.mount()
            d = os.path.join(self.mnt, f"b{n}")
            t0 = time.time()
            self.fill(d, self.names(n))
            self.emit("result", name="populate", size=n, wall=round(time.time() - t0, 1))
            subprocess.run(["sync"])
            self.umount()
            self.mount()
            self.measure("big_ls_f", ["ls", "-f", d], n, cold=False)
            self.measure("big_ls_l", ["ls", "-l", d], n, cold=True)
            if os.environ.get("BIG_LISTS_ONLY"):
                # the listings and the cold resume only (item 7); the pins and the handover are not measured
                self.remount()
                r = subprocess.run([sys.executable, lt, "coldresume", d, str(n // 2)], capture_output=True, text=True)
                self.emit("result", name="cold_resume", size=n, line=r.stdout.strip())
                self.measure("big_ls_f_cold_scan_ref", ["ls", "-f", d], n, cold=True)
                self.umount()
                continue
            # pins: rm -rf-like walk (a held stream) and ls hold a directory open; RSS and bytes per listed name
            # the handover below needs a /dev/fuse session (a ring session refuses to detach)
            self.umount()
            self.mount({"CONSTELLATION_FUSE_TRANSPORT": "dev-fuse"})
            h = subprocess.Popen([sys.executable, lt, "hold", d, str(n // 2)], stdout=subprocess.PIPE,
                                 stdin=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if h.stdout.readline().strip() != "READY":
                self.emit("result", name="hold_failed", size=n, err=h.stderr.read()[-500:])
            st = self.snap()
            self.emit("result", name="big_pinned", size=n, readdir=st["readdir"], rss_kb=self.rss_peak_kb(),
                      pinned_bytes_per_name=round((st["readdir"].get("pinned_bytes") or 0) / max(1, n // 2), 2))
            # releasedir latency: mean of the op's service time while a cold scan runs
            sc = subprocess.Popen(["ls", "-f", d], stdout=subprocess.DEVNULL)
            r = subprocess.run([sys.executable, lt, "openclose", d, "2000"], capture_output=True, text=True)
            sc.wait()
            self.emit("result", name="releasedir", size=n, line=r.stdout.strip())
            # the handover with the pinned directory: time, and the biggest memfd the daemon held meanwhile
            sizes = []
            stop = threading.Event()

            def watch():
                while not stop.is_set():
                    try:
                        for fd in os.listdir(f"/proc/{self.pid}/fd"):
                            p = f"/proc/{self.pid}/fd/{fd}"
                            if os.readlink(p).startswith("/memfd:"):
                                sizes.append(os.stat(p).st_size)
                    except OSError:
                        pass
                    time.sleep(0.02)
            w = threading.Thread(target=watch)
            w.start()
            t0 = time.time()
            r = subprocess.run([self.bin, "daemon", "--upgrade", "--timeout-s", "900", "--state-dir", self.state],
                               env=self.env(), capture_output=True, text=True)
            took = time.time() - t0
            stop.set()
            w.join()
            self.emit("result", name="handover_pinned", size=n, wall=round(took, 3), rc=r.returncode,
                      max_memfd=max(sizes or [0]), out=(r.stdout + r.stderr).strip()[-300:])
            h.stdin.close()
            h.wait()
            # a cold resume: restart (no hints), seek to the middle, read one page
            self.remount()
            r = subprocess.run([sys.executable, lt, "coldresume", d, str(n // 2)], capture_output=True, text=True)
            self.emit("result", name="cold_resume", size=n, line=r.stdout.strip())
            self.measure("big_ls_f_cold_scan_ref", ["ls", "-f", d], n, cold=True)
            self.umount()
        self.write_mode = None

    def run(self, phases):
        self.emit("cell", bin=self.bin, mode=",".join(self.modes), kernel=os.uname().release, store=self.root,
                  uring=open("/sys/module/fuse/parameters/enable_uring").read().strip()
                  if os.path.exists("/sys/module/fuse/parameters/enable_uring") else "n/a")
        self.create_fs()
        try:
            if "list" in phases:
                self.phase_list()
            if "trees" in phases:
                self.phase_trees()
            if "write" in phases:
                self.phase_write()
            if "big" in phases:
                self.phase_big()
            if "handover" in phases and "big" not in phases:
                os.environ.setdefault("BIG_SIZES", "1000000")
                self.phase_big()
        finally:
            try:
                self.umount()
            except Exception:
                pass
            subprocess.run(["rm", "-rf", self.root])
            self.emit("cell_end")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("bin")
    ap.add_argument("store")
    ap.add_argument("tag")
    ap.add_argument("out")
    ap.add_argument("--mode", default="none", help="none, or a mode, or modes separated by commas")
    ap.add_argument("--phases", default="list,write")
    a = ap.parse_args()
    Cell(a).run(a.phases.split(","))
