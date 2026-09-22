#!/usr/bin/env python3
"""2-node smoke test: create a plain FS, mount on two hosts, write on one,
read on the other, confirm P2P direct connection + forwarding evidence.

Usage: python3 smoke.py <prefix> [nodeA] [nodeB]
"""
import sys
import time
import common as c

def main():
    prefix = sys.argv[1] if len(sys.argv) > 1 else f"constellation-verify-{c.utc_ts()}"
    nodeA = sys.argv[2] if len(sys.argv) > 2 else "a"
    nodeB = sys.argv[3] if len(sys.argv) > 3 else "b"
    name = "smoke1"
    nodes = [nodeA, nodeB]

    print(f"[smoke] prefix={prefix} nodes={nodes}")

    print("[smoke] fs create (plain, first node only)")
    res = c.fs_create_first(nodes, prefix, name, e2e=False, timeout=60)
    for n, r in res.items():
        print(f"  {n}: rc={r.returncode} timeout={r.timed_out}\n    out={r.stdout.strip()!r}\n    err={r.stderr.strip()!r}")
    if not all(r.ok() for r in res.values()):
        print("FS CREATE FAILED -- aborting smoke test")
        sys.exit(1)

    print("[smoke] mount on both nodes (both pass --s3 so the non-creator registers too)")
    mres = c.mount_fanout(nodes, name, prefix=prefix, timeout=60)
    for n, r in mres.items():
        print(f"  {n}: rc={r.returncode} timeout={r.timed_out}\n    out={r.stdout.strip()!r}\n    err={r.stderr.strip()!r}")
    if not all(r.ok() for r in mres.values()):
        print("MOUNT FAILED -- aborting")
        sys.exit(1)

    time.sleep(2)
    mnt = c.mount_point_for(name)

    print("[smoke] write a file on nodeA")
    wr = c.ssh_run(nodeA, f"echo 'hello-from-{nodeA}-{time.time()}' > {mnt}/smoketest.txt && sync && cat {mnt}/smoketest.txt", timeout=20)
    print(f"  write rc={wr.returncode} out={wr.stdout.strip()!r} err={wr.stderr.strip()!r}")

    print("[smoke] poll read on nodeB (up to 20s)")
    content = None
    for i in range(20):
        rr = c.ssh_run(nodeB, f"cat {mnt}/smoketest.txt 2>&1", timeout=10)
        if rr.returncode == 0 and "hello-from" in rr.stdout:
            content = rr.stdout.strip()
            print(f"  visible after {i+1}s: {content!r}")
            break
        time.sleep(1)
    else:
        print("  FAILED: nodeB never saw the write within 20s")
        print(f"  last attempt: rc={rr.returncode} out={rr.stdout!r} err={rr.stderr!r}")

    print("[smoke] write from nodeB (non-holder, tests forwarding), read from nodeA")
    wr2 = c.ssh_run(nodeB, f"echo 'hello2-from-{nodeB}-{time.time()}' > {mnt}/smoketest2.txt && sync", timeout=20)
    print(f"  write2 rc={wr2.returncode} err={wr2.stderr.strip()!r}")
    for i in range(20):
        rr2 = c.ssh_run(nodeA, f"cat {mnt}/smoketest2.txt 2>&1", timeout=10)
        if rr2.returncode == 0 and "hello2-from" in rr2.stdout:
            print(f"  visible on nodeA after {i+1}s: {rr2.stdout.strip()!r}")
            break
        time.sleep(1)
    else:
        print("  FAILED: nodeA never saw nodeB's write within 20s")

    print("[smoke] status on both nodes")
    for n in nodes:
        st = c.status(n, name, timeout=20)
        print(f"--- status {n} ---")
        print(st.stdout)
        print(st.stderr)

    print("[smoke] daemon logs (tail) on both nodes")
    for n in nodes:
        lg = c.ssh_run(n, f"{c.REMOTE_BIN} log tail {name} --lines 200 2>&1", timeout=20)
        print(f"--- log {n} ---")
        print(lg.stdout[-4000:])
        print(lg.stderr[-2000:])

    print("[smoke] cleanup: umount both")
    ur = c.umount_fanout(nodes, name, timeout=60)
    for n, r in ur.items():
        print(f"  umount {n}: rc={r.returncode} out={r.stdout.strip()!r} err={r.stderr.strip()!r}")

if __name__ == "__main__":
    main()
