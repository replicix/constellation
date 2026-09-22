"""Shared helpers for the remote real-S3 benchmark / verification drivers.

python3 stdlib only. Used by both Phase A (bench) and Phase B (adversarial
correctness) drivers -- keep this module generic (ssh fan-out, binary
install, fs create/mount/umount, quiesce-wait, log collection) and put
phase-specific workload logic in the phase scripts themselves.
"""
from __future__ import annotations

import concurrent.futures
import json
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

SSH_USER = "ubuntu"
SSH_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=10",
    "-o", "ServerAliveInterval=10",
    "-o", "ServerAliveCountMax=6",
    "-o", "StrictHostKeyChecking=accept-new",
]

# Fixed fleet. Keys are short node names used throughout results/logs.
HOSTS = {
    "a": "10.108.0.70",     # us-west-2a
    "b": "10.108.11.148",   # us-west-2b
    "c": "10.108.18.174",   # us-west-2c
    "d": "10.108.6.98",     # us-west-2a
}
NODE_NAMES = list(HOSTS.keys())

BUCKET = "attila-test-211125321544-us-west-2-an"
AWS_REGION = "us-west-2"

REMOTE_BIN = "/usr/local/bin/constellation"
# Root fs *is* the 100GB NVMe (no separate /mnt volume on these hosts), and
# ubuntu has no sudo-free write access under /mnt -- keep everything under
# the home dir instead.
REMOTE_STATE_ROOT = "/home/ubuntu/cbench"
REMOTE_MOUNT_ROOT = "/home/ubuntu/cbench/mnt"


@dataclass
class RunResult:
    node: str
    returncode: int
    stdout: str
    stderr: str
    duration_s: float
    timed_out: bool = False

    def ok(self) -> bool:
        return self.returncode == 0 and not self.timed_out


def _ip(node: str) -> str:
    if node in HOSTS:
        return HOSTS[node]
    return node  # allow passing a raw IP


def ssh_run(node: str, remote_cmd: str, timeout: float = 30, env: Optional[dict] = None) -> RunResult:
    """Run remote_cmd on `node` via ssh, blocking, with a hard timeout."""
    ip = _ip(node)
    prefix = ""
    if env:
        prefix = " ".join(f"{k}={shlex.quote(v)}" for k, v in env.items()) + " "
    full_cmd = prefix + remote_cmd
    cmd = ["ssh"] + SSH_OPTS + [f"{SSH_USER}@{ip}", full_cmd]
    t0 = time.time()
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return RunResult(node, p.returncode, p.stdout, p.stderr, time.time() - t0)
    except subprocess.TimeoutExpired as e:
        out = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        err = e.stderr.decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
        return RunResult(node, -1, out, err, time.time() - t0, timed_out=True)


def ssh_fanout(nodes, remote_cmd: str, timeout: float = 30, env: Optional[dict] = None) -> dict:
    """Run the same remote_cmd on several nodes concurrently. Returns {node: RunResult}."""
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as ex:
        futs = {ex.submit(ssh_run, n, remote_cmd, timeout, env): n for n in nodes}
        for fut in concurrent.futures.as_completed(futs):
            n = futs[fut]
            results[n] = fut.result()
    return results


def ssh_fanout_cmds(node_cmds: dict, timeout: float = 30, env: Optional[dict] = None) -> dict:
    """Run a different remote_cmd per node concurrently. node_cmds: {node: cmd}."""
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(node_cmds)) as ex:
        futs = {ex.submit(ssh_run, n, c, timeout, env): n for n, c in node_cmds.items()}
        for fut in concurrent.futures.as_completed(futs):
            n = futs[fut]
            results[n] = fut.result()
    return results


def scp_to(node: str, local_path: str, remote_path: str, timeout: float = 300) -> RunResult:
    ip = _ip(node)
    cmd = ["scp"] + SSH_OPTS + [local_path, f"{SSH_USER}@{ip}:{remote_path}"]
    t0 = time.time()
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return RunResult(node, p.returncode, p.stdout, p.stderr, time.time() - t0)
    except subprocess.TimeoutExpired as e:
        return RunResult(node, -1, "", str(e), time.time() - t0, timed_out=True)


def scp_from(node: str, remote_path: str, local_path: str, timeout: float = 120) -> RunResult:
    ip = _ip(node)
    cmd = ["scp"] + SSH_OPTS + [f"{SSH_USER}@{ip}:{remote_path}", local_path]
    t0 = time.time()
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return RunResult(node, p.returncode, p.stdout, p.stderr, time.time() - t0)
    except subprocess.TimeoutExpired as e:
        return RunResult(node, -1, "", str(e), time.time() - t0, timed_out=True)


# ---------------------------------------------------------------------------
# Install
# ---------------------------------------------------------------------------

def install_binary_if_needed(node: str, local_bin: str, expect_version: str, timeout: float = 300) -> RunResult:
    """scp+install the constellation binary on `node` unless already present
    with a matching `constellation --version` output. NOTE: the control-host
    to EC2 ssh/scp link in this sandbox is bandwidth-limited (~200-400 KB/s),
    so avoid re-transferring the ~43MB binary unless required."""
    check = ssh_run(node, "constellation --version 2>&1 || true", timeout=10)
    if expect_version in (check.stdout or ""):
        return check
    tmp = "/tmp/constellation_new"
    r = scp_to(node, local_bin, tmp, timeout=timeout)
    if not r.ok():
        return r
    return ssh_run(
        node,
        f"sudo mv {tmp} {REMOTE_BIN} && sudo chmod +x {REMOTE_BIN} && constellation --version",
        timeout=15,
    )


def install_fanout(nodes, local_bin: str, expect_version: str, timeout: float = 300) -> dict:
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as ex:
        futs = {
            ex.submit(install_binary_if_needed, n, local_bin, expect_version, timeout): n
            for n in nodes
        }
        for fut in concurrent.futures.as_completed(futs):
            results[futs[fut]] = fut.result()
    return results


# ---------------------------------------------------------------------------
# FS lifecycle
# ---------------------------------------------------------------------------

def s3_url(prefix: str) -> str:
    return f"s3://{BUCKET}/{prefix}"


def base_env(e2e_passphrase: Optional[str] = None) -> dict:
    env = {"AWS_REGION": AWS_REGION}
    if e2e_passphrase:
        env["CONSTELLATION_PASSPHRASE"] = e2e_passphrase
    return env


def fs_create(node: str, prefix: str, name: str, e2e: bool = False,
              e2e_passphrase: Optional[str] = None, extra_args: str = "",
              timeout: float = 60) -> RunResult:
    url = s3_url(prefix)
    flag = "--e2e " if e2e else ""
    cmd = f"{REMOTE_BIN} fs create --s3 {url} {flag}{extra_args} {name}"
    return ssh_run(node, cmd, timeout=timeout, env=base_env(e2e_passphrase if e2e else None))


def fs_create_first(nodes, prefix: str, name: str, e2e: bool = False,
                     e2e_passphrase: Optional[str] = None, timeout: float = 60) -> dict:
    """Only the FIRST node runs `fs create` (creates the backend fs and
    registers locally). Every other node must NOT also run `fs create`:
    the CLI's `fs create` unconditionally calls `store.create_fs()` and
    errors "filesystem already exists at this prefix" if it's already
    there -- there is no idempotent "just register" mode for `fs create`
    itself. The other nodes instead register by passing `--s3` on their
    first `mount` call (see `mount()` below), which is the CLI's actual
    supported "join an existing name" path."""
    first = nodes[0]
    return {first: fs_create(first, prefix, name, e2e, e2e_passphrase, timeout=timeout)}


def state_dir_for(name: str) -> str:
    """The CLI's own default registered-name state dir (we never pass
    --state-dir for named mounts: doing so forces the *ad-hoc, unregistered*
    mount path -- see mount() docstring -- which would defeat cross-node
    name registration)."""
    return f"/home/{SSH_USER}/.local/share/constellation/{name}"


def mount_point_for(name: str) -> str:
    return f"{REMOTE_MOUNT_ROOT}/{name}"


def mount(node: str, name: str, prefix: Optional[str] = None, cache_size: Optional[str] = None,
          write_mode: Optional[str] = None, fsync_mode: Optional[str] = None,
          e2e_passphrase: Optional[str] = None, extra_args: str = "",
          timeout: float = 60) -> RunResult:
    """Mount `name` on `node`, using the CLI's registered-name path (NOT
    --state-dir, which forces an ad-hoc/unregistered mount instead of the
    Named/registry path -- see cmd_mount in crates/cli/src/main.rs). Pass
    `prefix` (the S3 prefix, same value used in fs_create_first) on EVERY
    node's first mount for this name -- this is how a node other than the
    fs-create node registers the name locally (`mount --s3 <url> NAME
    MOUNTPOINT`); harmless to also pass it on the creator node since it
    already matches the registry entry."""
    mnt = mount_point_for(name)
    args = []
    if prefix:
        args.append(f"--s3 {s3_url(prefix)}")
    if cache_size:
        args.append(f"--cache-size {cache_size}")
    if write_mode:
        args.append(f"--write-mode {write_mode}")
    if fsync_mode:
        args.append(f"--fsync-mode {fsync_mode}")
    if extra_args:
        args.append(extra_args)
    cmd = (
        f"mkdir -p {mnt} && "
        f"{REMOTE_BIN} mount {' '.join(args)} {name} {mnt}"
    )
    return ssh_run(node, cmd, timeout=timeout, env=base_env(e2e_passphrase))


def mount_fanout(nodes, name: str, **kwargs) -> dict:
    timeout = kwargs.pop("timeout", 60)
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as ex:
        futs = {ex.submit(mount, n, name, timeout=timeout, **kwargs): n for n in nodes}
        for fut in concurrent.futures.as_completed(futs):
            results[futs[fut]] = fut.result()
    return results


def set_write_mode(node: str, name: str, mode: str, timeout: float = 30) -> RunResult:
    return ssh_run(node, f"{REMOTE_BIN} write-mode {name} {mode} 2>&1", timeout=timeout)


def set_write_mode_fanout(nodes, name: str, mode: str, timeout: float = 30) -> dict:
    return ssh_fanout(nodes, f"{REMOTE_BIN} write-mode {name} {mode} 2>&1", timeout=timeout)


def umount(node: str, name: str, timeout: float = 60) -> RunResult:
    return ssh_run(node, f"{REMOTE_BIN} umount {name} 2>&1; true", timeout=timeout)


def umount_fanout(nodes, name: str, timeout: float = 60) -> dict:
    return ssh_fanout(nodes, f"{REMOTE_BIN} umount {name} 2>&1; true", timeout=timeout)


def force_cleanup(node: str, name: str, timeout: float = 30) -> RunResult:
    """Best-effort: umount, fusermount -uz, kill stray daemons."""
    mnt = mount_point_for(name)
    cmd = (
        f"{REMOTE_BIN} umount {name} >/tmp/umount_{name}.log 2>&1; "
        f"fusermount3 -uz {mnt} 2>/dev/null; fusermount -uz {mnt} 2>/dev/null; "
        f"sudo umount -l {mnt} 2>/dev/null; "
        f"pkill -9 -f 'constellation mount.*{name}' 2>/dev/null; true"
    )
    return ssh_run(node, cmd, timeout=timeout)


# ---------------------------------------------------------------------------
# status / quiesce
# ---------------------------------------------------------------------------

def status(node: str, name: str, timeout: float = 20) -> RunResult:
    return ssh_run(node, f"{REMOTE_BIN} status {name} 2>&1", timeout=timeout)


def status_fanout(nodes, name: str, timeout: float = 20) -> dict:
    return ssh_fanout(nodes, f"{REMOTE_BIN} status {name} 2>&1", timeout=timeout)


def parse_status_json(text: str) -> Optional[dict]:
    """`constellation status NAME` prints a log preamble (tracing INFO line,
    possibly ANSI-colored) followed by one JSON object. Find the first '{'
    and decode just the first JSON value from there, ignoring anything
    after it."""
    idx = text.find("{")
    if idx < 0:
        return None
    try:
        obj, _end = json.JSONDecoder().raw_decode(text[idx:])
        return obj
    except json.JSONDecodeError:
        return None


def status_json(node: str, name: str, timeout: float = 20) -> Optional[dict]:
    r = status(node, name, timeout=timeout)
    return parse_status_json(r.stdout + r.stderr)


def spool_backlog(status_text: str) -> Optional[int]:
    """Pull journal_backlog + pending_uploads out of a `status` JSON blob
    (0 means fully shipped/quiesced for that node). None if unparseable."""
    obj = parse_status_json(status_text)
    if obj is None:
        return None
    try:
        backlog = obj.get("spool", {}).get("journal_backlog", 0)
        pending = obj.get("writeback", {}).get("pending_uploads", 0)
        return int(backlog) + int(pending)
    except (TypeError, ValueError, AttributeError):
        return None


def wait_quiesce(nodes, name: str, timeout_s: float = 120, poll_s: float = 2.0) -> tuple:
    """Poll `status` on every node until every node's (journal_backlog +
    pending_uploads) reads 0. Returns (quiesced: bool, last_status: {node: str})."""
    deadline = time.time() + timeout_s
    last_texts = {}
    while time.time() < deadline:
        results = status_fanout(nodes, name, timeout=15)
        backlogs = {}
        for n, r in results.items():
            last_texts[n] = r.stdout + r.stderr
            backlogs[n] = spool_backlog(last_texts[n])
        if backlogs and all(v == 0 for v in backlogs.values()):
            return True, last_texts
        time.sleep(poll_s)
    return False, last_texts


# ---------------------------------------------------------------------------
# logs
# ---------------------------------------------------------------------------

def collect_logs(node: str, name: str, out_dir: str, tail_lines: int = 2000) -> str:
    """Fetch `constellation log tail` output (small, text) via ssh stdout
    capture -- avoids a slow scp of the raw log files over the constrained
    control link."""
    r = ssh_run(node, f"{REMOTE_BIN} log tail {name} --lines {tail_lines} 2>&1", timeout=30)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"log-{node}-{name}.txt")
    with open(path, "w") as f:
        f.write(r.stdout)
        f.write(r.stderr)
    return path


def collect_status(node: str, name: str, out_dir: str) -> str:
    r = status(node, name, timeout=20)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"status-{node}-{name}.txt")
    with open(path, "w") as f:
        f.write(r.stdout)
        f.write(r.stderr)
    return path


# ---------------------------------------------------------------------------
# worker (bench_worker.py) fan-out
# ---------------------------------------------------------------------------

REMOTE_WORKER_PATH = "/home/ubuntu/cbench/bench_worker.py"


def deploy_worker(nodes, local_worker_path: str, timeout: float = 60) -> dict:
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as ex:
        def do(n):
            ssh_run(n, "mkdir -p /home/ubuntu/cbench", timeout=15)
            return scp_to(n, local_worker_path, REMOTE_WORKER_PATH, timeout=timeout)
        futs = {ex.submit(do, n): n for n in nodes}
        for fut in concurrent.futures.as_completed(futs):
            results[futs[fut]] = fut.result()
    return results


def run_worker(node: str, mode: str, name: str, start_at: float, out_path: str,
                extra_args: str = "", timeout: float = 600) -> RunResult:
    mnt = mount_point_for(name)
    cmd = (
        f"python3 {REMOTE_WORKER_PATH} {mode} --mount {mnt} --node {node} "
        f"--start-at {start_at} --out {out_path} {extra_args} 2>&1"
    )
    return ssh_run(node, cmd, timeout=timeout)


def run_worker_fanout(nodes, mode: str, name: str, start_at: float, out_path: str,
                       extra_args: str = "", timeout: float = 600) -> dict:
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as ex:
        futs = {
            ex.submit(run_worker, n, mode, name, start_at, out_path, extra_args, timeout): n
            for n in nodes
        }
        for fut in concurrent.futures.as_completed(futs):
            results[futs[fut]] = fut.result()
    return results


def fetch_worker_result(node: str, out_path: str) -> Optional[dict]:
    r = ssh_run(node, f"cat {out_path} 2>&1", timeout=20)
    if r.returncode != 0:
        return None
    try:
        return json.loads(r.stdout)
    except json.JSONDecodeError:
        return None


def utc_ts() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def dump_json(obj, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, default=str)
