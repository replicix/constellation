#!/usr/bin/env python3
"""Plan 45 section 7, the box gates: one cell = one build, one backing file system.

  cell.py BIN STOREDIR TAG OUT [--phases link,burst,control,list,trees]
  cell.py creator DIR STOPFILE OUTFILE PACE_US          (internal: the concurrent `create` loop)

  BIN       constellation binary (H0, the baseline build, or the build under test)
  STOREDIR  directory on the file system the state dir and the file store live on (ext4, xfs, btrfs, tmpfs);
            everything of the cell goes in STOREDIR/p45-TAG and is removed at the end
  OUT       result file: one JSON object per line (kind "run", "result", "load", "cell_start", "cell_end")

Every mount is `CONSTELLATION_FUSE_TRANSPORT=dev-fuse` (a plain /dev/fuse session), a file:// backend, and a fresh
file system per phase, so one phase's tree never sits under another's measurement.

Phases (the gates of plan 45 section 7, "Box"; `control` is a reference, not a gate):
  link    one `stress-ng --link 1 --link-ops 1` bogo-op (8,192 links then 8,193 unlinks of one file): its wall time;
          then `meta.db` bytes and `.jnl` files (mounted, as the harness reads them); then kill -9 and the time
          the next mount takes (it replays the journals), then a clean unmount and mount of the recovered state.
  burst   8,192 links of one file over two directories, then the unlinks, while a second process creates files in
          a third directory in a loop: the creates' p50/p99/max before the burst (idle), during the link burst and
          during the unlink burst; the links' and unlinks' own p50 at 2,000 names.
  control the burst phase with single-name creates and unlinks in place of the links and unlinks.
  list    a 1,000-file directory of `nlink` 1: `ls -la`, `find -ls`, `du -s`, cold (a fresh mount for every
          repetition: the kernel's and the daemon's caches empty, the OS page cache warm) and warm (the same mount,
          after one untimed pass; REPS back-to-back runs).
  trees   (i) a pnpm-like tree: 10,000 files with 10 names each, spread over 1,000 package directories of 100;
          (ii) an rsnapshot-like tree: 50,000 files in 500 directories, then six `cp -al` generations of it (7
          names per file). `ls -laR` of the pnpm tree and of the last generation, cold and warm; the cold ones
          are reported, only the warm ones are gated.

Environment (optional): REPS (default 9, the small directory), BIG_COLD (3), BIG_WARM (5), LOAD_MAX (1-minute load
gate before each phase, default 4), PHASE_WAIT (seconds a gate waits before it records gate_ok=false, default 900),
BURST_N (8192), BURST_ROUNDS (link+unlink cycles, default 1; more rounds give the p99 more samples), CONTROL_ROUNDS (3), PACE_US (the creator's pause between creates, default 200), MOUNT_WAIT (seconds, default 900: the
H0 build's remount after a burst takes minutes).
"""
import argparse
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))


def load1():
    return float(open("/proc/loadavg").read().split()[0])


def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return None
    return xs[min(len(xs) - 1, int(p / 100.0 * len(xs)))]


def creator(d, stop, out, pace_us):
    """Create files in `d` until `stop` exists; every create's latency (ns, the open(O_CREAT|O_EXCL) alone) goes to
    `out` with its start time (ns since the loop began) so the caller can cut the samples by phase."""
    os.makedirs(d, exist_ok=True)
    pace = pace_us / 1e6
    samples = []
    t00 = time.perf_counter_ns()
    i = 0
    while not os.path.exists(stop):
        p = os.path.join(d, "c%08d" % i)
        t0 = time.perf_counter_ns()
        fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        t1 = time.perf_counter_ns()
        os.close(fd)
        samples.append((t0 - t00, t1 - t0))
        i += 1
        if pace:
            time.sleep(pace)
    with open(out, "w") as f:
        json.dump({"t00": t00, "s": samples}, f)


class Cell:
    def __init__(self, a):
        self.bin = os.path.abspath(a.bin)
        self.tag = a.tag
        self.root = os.path.join(os.path.abspath(a.store), "p45-" + a.tag)
        self.out = open(a.out, "a", buffering=1)
        self.mnt = os.path.join(self.root, "mnt")
        self.state = os.path.join(self.root, "state")
        self.backend = os.path.join(self.root, "backend")
        self.home = os.path.join(self.root, "home")
        self.log = os.path.join(self.root, "mount.log")
        self.proc = None
        self.load_max = float(os.environ.get("LOAD_MAX", "4"))
        self.phase_wait = int(os.environ.get("PHASE_WAIT", "900"))
        self.reps = int(os.environ.get("REPS", "9"))
        self.big_cold = int(os.environ.get("BIG_COLD", "3"))
        self.big_warm = int(os.environ.get("BIG_WARM", "5"))
        self.mount_wait = int(os.environ.get("MOUNT_WAIT", "900"))
        self.burst_n = int(os.environ.get("BURST_N", "8192"))
        self.pace_us = int(os.environ.get("PACE_US", "200"))
        self.rounds = int(os.environ.get("BURST_ROUNDS", "1"))
        self.rng = random.Random(45)

    # ------------------------------------------------------------ records
    def emit(self, kind, **kw):
        kw.update(kind=kind, tag=self.tag, t=int(time.time()))
        self.out.write(json.dumps(kw, sort_keys=True) + "\n")

    def gate(self, name):
        t0 = time.time()
        ok = True
        while load1() >= self.load_max:
            if time.time() - t0 > self.phase_wait:
                ok = False
                break
            time.sleep(10)
        self.emit("load", phase=name, load1=load1(), waited=int(time.time() - t0), gate_ok=ok)

    # ------------------------------------------------------------- daemon
    def env(self):
        return dict(os.environ, HOME=self.home, CONSTELLATION_FUSE_TRANSPORT="dev-fuse")

    def create_fs(self):
        if self.proc:
            self.umount()
        # a daemon of the phase before may still be closing files under its state dir (seen on tmpfs): `rm -rf`
        # then leaves the directory behind, so repeat it until the root is gone
        for _ in range(40):
            subprocess.run(["rm", "-rf", self.root])
            if not os.path.exists(self.root):
                break
            time.sleep(0.5)
        for d in (self.home, self.state, self.backend, self.mnt):
            os.makedirs(d)
        r = subprocess.run([self.bin, "fs", "create", "bench", "--s3", "file://" + self.backend,
                            "--chunk-size", "1048576"], env=self.env(), capture_output=True, text=True, timeout=300)
        if r.returncode:
            raise SystemExit("fs create failed: " + r.stderr)

    def mount(self):
        """Mount and return the seconds until the mount point answers."""
        cmd = [self.bin, "mount", "/", self.mnt, "--s3", "file://" + self.backend, "--state-dir", self.state,
               "--cache-size", "1G", "--foreground"]
        t0 = time.perf_counter()
        self.proc = subprocess.Popen(["timeout", "--kill-after=10s", "14400"] + cmd, env=self.env(),
                                     stdout=open(self.log, "ab"), stderr=subprocess.STDOUT)
        while time.perf_counter() - t0 < self.mount_wait:
            if os.path.ismount(self.mnt):
                break
            time.sleep(0.01)
        else:
            raise SystemExit("mount failed: " + open(self.log).read()[-2000:])
        up = time.perf_counter() - t0
        # `timeout` is the child; the daemon is its child.
        self.pid = int(subprocess.check_output(["pgrep", "-P", str(self.proc.pid)]).split()[0])
        return up

    def umount(self):
        subprocess.run(["fusermount3", "-u", self.mnt], capture_output=True)
        if self.proc:
            try:
                self.proc.wait(timeout=600)
            except subprocess.TimeoutExpired:
                self.proc.terminate()
                self.proc.wait(timeout=60)
        self.proc = None

    def crash(self):
        """kill -9 the daemon and take the dead mount away."""
        os.kill(self.pid, signal.SIGKILL)
        self.proc.wait(timeout=60)
        self.proc = None
        for _ in range(100):
            subprocess.run(["fusermount3", "-u", "-z", self.mnt], capture_output=True)
            if not os.path.ismount(self.mnt):
                break
            time.sleep(0.1)

    def remount(self):
        self.umount()
        return self.mount()

    def status(self):
        r = subprocess.run([self.bin, "status", "/", "--state-dir", self.state], env=self.env(),
                           capture_output=True, text=True, timeout=120)
        return json.loads(r.stdout)

    def fuse_ops(self):
        st = self.status()
        ops = {}
        for s in st["vfs_ops"]["series"]:
            if s["frontend"] == "fuse":
                ops[s["op"]] = ops.get(s["op"], 0) + sum(s["outcomes"].values())
        return ops

    def meta_bytes(self):
        """Bytes under the state dir's meta.db (a directory of segments and journals) and its `.jnl` count."""
        size = jnl = 0
        for dp, _, fns in os.walk(self.state):
            if "meta.db" not in dp.split(os.sep) and not dp.endswith("meta.db"):
                continue
            for fn in fns:
                try:
                    size += os.path.getsize(os.path.join(dp, fn))
                except OSError:
                    pass
                if fn.endswith(".jnl"):
                    jnl += 1
        return size, jnl

    def meta_files(self, top=8):
        """The largest files under meta.db, `relative path: bytes`, for the record."""
        fs = []
        for dp, _, fns in os.walk(self.state):
            if "meta.db" not in dp.split(os.sep):
                continue
            for fn in fns:
                try:
                    fs.append((os.path.getsize(os.path.join(dp, fn)), os.path.relpath(os.path.join(dp, fn), self.state)))
                except OSError:
                    pass
        fs.sort(reverse=True)
        return {"files": len(fs), "top": ["%s:%d" % (n, b) for b, n in fs[:top]]}

    def state_bytes(self):
        n = 0
        for dp, _, fns in os.walk(self.state):
            for fn in fns:
                try:
                    n += os.path.getsize(os.path.join(dp, fn))
                except OSError:
                    pass
        return n

    # ------------------------------------------------------------ measure
    def measure(self, name, cmd, size, mode, reps, shell=False):
        """`reps` runs of `cmd`. cold: a fresh mount for every run. warm: one untimed pass, then back to back."""
        if mode == "warm":
            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=shell)
        for rep in range(reps):
            if mode == "cold":
                self.remount()
            o0 = self.fuse_ops()
            t0 = time.perf_counter()
            r = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=shell)
            wall = time.perf_counter() - t0
            o1 = self.fuse_ops()
            reqs = {k: o1.get(k, 0) - o0.get(k, 0) for k in set(o1) | set(o0)}
            reqs = {k: v for k, v in reqs.items() if v}
            self.emit("run", name=name, mode=mode, size=size, rep=rep, wall=round(wall, 4), rc=r.returncode,
                      requests=sum(reqs.values()), reqs=reqs)

    # -------------------------------------------------------------- phases
    def phase_link(self):
        self.gate("link")
        self.create_fs()
        self.mount()
        d = os.path.join(self.mnt, "sng")
        os.makedirs(d)
        t0 = time.perf_counter()
        r = subprocess.run(["timeout", "--kill-after=10s", "900", "stress-ng", "--link", "1", "--link-ops", "1",
                            "--temp-path", d, "--metrics-brief"], capture_output=True, text=True)
        wall = time.perf_counter() - t0
        bytes_, jnl = self.meta_bytes()
        self.emit("result", name="stress_ng_link_bogo_op", wall=round(wall, 3), rc=r.returncode,
                  meta_db_bytes=bytes_, jnl_files=jnl, state_bytes=self.state_bytes(), meta_files=self.meta_files(),
                  stress_ng_tail=(r.stdout + r.stderr)[-600:])
        # the next mount after a crash (kill -9): the journals of the bogo-op are replayed by it
        self.crash()
        bytes_, jnl = self.meta_bytes()
        up = self.mount()
        self.emit("result", name="remount_after_crash", wall=round(up, 3), meta_db_bytes_before=bytes_,
                  jnl_files_before=jnl)
        # and after a clean unmount of the recovered state
        self.umount()
        up = self.mount()
        self.emit("result", name="remount_clean_after_recovery", wall=round(up, 3))
        self.umount()

    def phase_control(self):
        """The same burst with single-name creates and unlinks instead of links: what any write burst costs a
        concurrent create on this file system (the reference the link burst's tail is read against)."""
        self.phase_burst("control")

    def phase_burst(self, kind="link"):
        self.gate("burst" if kind == "link" else "control")
        self.create_fs()
        self.mount()
        n = self.burst_n
        rounds = self.rounds if kind == "link" else int(os.environ.get("CONTROL_ROUNDS", "3"))
        src = os.path.join(self.mnt, "src")
        open(src, "w").write("x" * 4096)
        dirs = [os.path.join(self.mnt, "ld0"), os.path.join(self.mnt, "ld1")]
        for d in dirs:
            os.makedirs(d)
        stop = os.path.join(self.root, "stop")
        out = os.path.join(self.root, "creates.json")
        if os.path.exists(stop):
            os.unlink(stop)
        cr = subprocess.Popen([sys.executable, os.path.abspath(__file__), "creator", os.path.join(self.mnt, "cr"),
                               stop, out, str(self.pace_us)])
        time.sleep(3)
        mark = {"idle_end": time.perf_counter_ns()}  # compared on the creator's clock below via its t00
        # the creator's clock: perf_counter_ns is CLOCK_MONOTONIC, shared by both processes
        links, unlinks = [], []
        lwin, uwin = [], []  # (start, end) of every round's link burst and unlink burst, creator's clock
        for rnd in range(rounds):
            t_link0 = time.perf_counter_ns()
            for i in range(n):
                p = os.path.join(dirs[i & 1], "l%06d" % i)
                t0 = time.perf_counter_ns()
                if kind == "link":
                    os.link(src, p)
                else:
                    os.close(os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644))
                if rnd == 0:
                    links.append(time.perf_counter_ns() - t0)
            t_link1 = time.perf_counter_ns()
            t_ul0 = time.perf_counter_ns()
            for i in range(n):
                p = os.path.join(dirs[i & 1], "l%06d" % i)
                t0 = time.perf_counter_ns()
                os.unlink(p)
                if rnd == 0:
                    unlinks.append(time.perf_counter_ns() - t0)
            t_ul1 = time.perf_counter_ns()
            lwin.append((t_link0, t_link1))
            uwin.append((t_ul0, t_ul1))
        time.sleep(1)
        open(stop, "w").close()
        cr.wait(timeout=600)
        c = json.load(open(out))
        t00 = c["t00"]
        s = [(t00 + a, b) for a, b in c["s"]]

        def window(wins):
            xs = [b for a, b in s if any(lo <= a < hi for lo, hi in wins)]
            return {"n": len(xs), "p50_us": round((pct(xs, 50) or 0) / 1e3, 1), "p99_us": round((pct(xs, 99) or 0) / 1e3, 1),
                    "max_us": round(max(xs, default=0) / 1e3, 1)}

        self.emit("result", name="creates_during_burst" if kind == "link" else "creates_during_control",
                  idle=window([(t00, mark["idle_end"])]), link_burst=window(lwin),
                  unlink_burst=window(uwin), links=n, rounds=rounds,
                  link_burst_s=round((lwin[0][1] - lwin[0][0]) / 1e9, 3),
                  unlink_burst_s=round((uwin[0][1] - uwin[0][0]) / 1e9, 3))
        k = min(2000, n - 50)
        self.emit("result", name="link_unlink_latency" if kind == "link" else "control_latency",
                  link_p50_at_2000_us=round(pct(links[k - 50:k + 50], 50) / 1e3, 1),
                  link_p50_first_100_us=round(pct(links[:100], 50) / 1e3, 1),
                  link_p50_last_100_us=round(pct(links[-100:], 50) / 1e3, 1),
                  unlink_p50_first_100_us=round(pct(unlinks[:100], 50) / 1e3, 1),
                  unlink_p50_at_2000_names_us=round(pct(unlinks[n - k - 50:n - k + 50], 50) / 1e3, 1))
        bytes_, jnl = self.meta_bytes()
        self.emit("result", name="burst_meta" if kind == "link" else "control_meta", meta_db_bytes=bytes_, jnl_files=jnl, meta_files=self.meta_files())
        self.umount()

    def phase_list(self):
        self.gate("list")
        self.create_fs()
        self.mount()
        d = os.path.join(self.mnt, "d1k")
        os.makedirs(d)
        for i in range(1000):
            with open(os.path.join(d, "file%04d" % i), "w") as f:
                f.write("x" * 100)
        self.remount()
        for name, cmd in (("ls_la", ["ls", "-la", d]), ("find_ls", ["find", d, "-ls"]), ("du_s", ["du", "-s", d])):
            for mode in ("cold", "warm"):
                self.measure("d1k_" + name, cmd, 1000, mode, self.reps)
        self.umount()

    def phase_trees(self):
        self.gate("trees")
        self.create_fs()
        self.mount()
        # pnpm-like: file j has its 10 names in directories (j + 100 k) mod 1000, k = 0..9: every directory
        # gets exactly 100 names, the 10 names of a file are in 10 different directories.
        root = os.path.join(self.mnt, "pnpm")
        os.makedirs(root)
        pk = [os.path.join(root, "pkg%04d" % i) for i in range(1000)]
        for p in pk:
            os.mkdir(p)
        t0 = time.time()
        for j in range(10000):
            first = os.path.join(pk[j % 1000], "f%05d" % j)
            with open(first, "w") as f:
                f.write("x" * 64)
            for k in range(1, 10):
                os.link(first, os.path.join(pk[(j + 100 * k) % 1000], "f%05d" % j))
        self.emit("result", name="pnpm_populate", wall=round(time.time() - t0, 1), names=100000)
        # rsnapshot-like: 50,000 files in 500 directories, then six `cp -al` generations
        snap = os.path.join(self.mnt, "snap")
        g0 = os.path.join(snap, "gen0")
        os.makedirs(g0)
        t0 = time.time()
        for i in range(500):
            dd = os.path.join(g0, "d%03d" % i)
            os.mkdir(dd)
            for j in range(100):
                with open(os.path.join(dd, "f%03d" % j), "w") as f:
                    f.write("x" * 64)
        for g in range(1, 7):
            subprocess.run(["cp", "-al", g0, os.path.join(snap, "gen%d" % g)], check=True)
        self.emit("result", name="rsnapshot_populate", wall=round(time.time() - t0, 1), names=350000)
        self.remount()
        last = os.path.join(snap, "gen6")
        for name, cmd, size in (("pnpm_ls_laR", ["ls", "-laR", root], 100000),
                                ("rsnap_ls_laR", ["ls", "-laR", last], 50000)):
            self.measure(name, cmd, size, "cold", self.big_cold)
            self.measure(name, cmd, size, "warm", self.big_warm)
        self.umount()

    def run(self, phases):
        self.emit("cell_start", bin=self.bin, phases=phases, load1=load1())
        try:
            for ph in phases:
                getattr(self, "phase_" + ph)()
        finally:
            if self.proc:
                self.umount()
            shutil.rmtree(self.root, ignore_errors=True)
        self.emit("cell_end", load1=load1())


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "creator":
        creator(sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5]))
        return
    ap = argparse.ArgumentParser()
    ap.add_argument("bin")
    ap.add_argument("store")
    ap.add_argument("tag")
    ap.add_argument("out")
    ap.add_argument("--phases", default="link,burst,control,list,trees")
    a = ap.parse_args()
    Cell(a).run(a.phases.split(","))


if __name__ == "__main__":
    main()
