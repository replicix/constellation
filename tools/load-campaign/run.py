#!/usr/bin/env python3
"""Host-load campaign runner: a phase/profile/scenario/seed matrix of
`harness run`, one harness process per cell, each under a load profile
(CONSTELLATION_HARNESS_LOAD, crates/harness/src/load.rs) with every
invariant checker on (CONSTELLATION_HARNESS_VERIFY=1,
crates/harness/src/verify.rs), its results JSON, harness log and every
daemon log kept per run, then a summary.

    tools/load-campaign/run.py --out DIR --phase smoke
    tools/load-campaign/run.py --out DIR --phase 3 --profile cpu25 --seeds 42,43,44
    tools/load-campaign/run.py --out DIR --scenario lock-failover \\
        --profile 'cpu:25@a' --profile baseline --seeds 1
    tools/load-campaign/run.py --summarize DIR

Layout: DIR/<phase>/<profile>/<scenario>/seed-<n>/{results.json,
harness.log, logs/<scenario>/<node>/mount.log*, run.json}; DIR/summary.json
and DIR/summary.md. A cell's verdict is the scenario's outcome (`passed`,
`inconclusive`, `failed`, `skipped`), or `harness-error` when the harness
itself did not finish (killed at --wall), or, for a smoke break run,
`caught`/`missed`. The exit status is 1 when any cell failed (or a break
was missed), or when more than `--max-inconclusive-pct` (10) of the judged
cells are `inconclusive` (not a pass: its bounds could not be judged on a
starved host, or part of it was skipped), else 0.

Profiles are the harness's (presets: `harness` load.rs PRESETS, or items
like `cpu:25@a,s3:latency:500:400`), plus the runner's own `dm-delay`
item (the run's TMPDIR on a dm-delay ext4 disk: 20 ms reads, 100 ms writes
unless --dm-delay-ms says otherwise), e.g. `dm-delay` or `dm-delay,hogs`.

On a shared host, --notice FILE gets a line per run while it injects load
(removed when it ends), so others can discount their timing results.
"""

from __future__ import annotations

import argparse
import fcntl
import fnmatch
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent

# Load campaign §5.3 (lone node) and §5.4 (clusters); the soak (§5.5) is
# inject.sh rotate beside the chaos coordinator.
PHASES: dict[str, dict] = {
    "smoke": {
        "scenarios": ["load-probe"],
        "profiles": [
            "baseline",
            "cpu25",
            "cpu-bursty",
            "hogs:cpu=4:vm=1:vmbytes=256m:io=1:hdd=1:hddbytes=64m",
            "runtime-hog",
            "store-stall:3000:8000",
            "log-stall:3000:8000",
            "step-pause:2000:8000@b",
            "pause:lock-lapse+500:10000@b",
            "s3-jitter",
            "s3:blackhole:5000:5000",
            "net-wan",
            "net-flap",
            "clock-skew",
            "dm-delay",
        ],
        # (break, the failure's reason must match, profile): each checker
        # catches its own deliberate violation; `inconclusive` instead of a
        # pattern: the run must end inconclusive (the same latency break,
        # under injected load, is not a failure).
        "breaks": [
            ("data", r"corrupt", "baseline"),
            ("ledger", r"lost acknowledged write", "baseline"),
            ("exclusion", r"lock exclusion", "baseline"),
            ("token", r"fencing token order", "baseline"),
            ("split-brain", r"split brain", "baseline"),
            ("log-drop", r"log line\(s\) dropped", "baseline"),
            ("errno", r"outside the documented set", "baseline"),
            ("enoent", r"enoent-owned", "baseline"),
            ("stall", r"hang: ", "baseline"),
            ("log-missing", r"log missing", "baseline"),
            # Past the hard cap: a failure under any profile.
            ("latency", r"past the hard cap", "baseline"),
            ("latency", r"past the hard cap", "cpu25"),
            # Within the cap, unexplained by the probe: a failure without a
            # probe-blind injector (cpu throttling is measured)...
            ("latency-mild", r"does not explain the excess", "baseline"),
            ("latency-mild", r"does not explain the excess", "cpu25"),
            # ... and inconclusive under one that applied.
            ("latency-mild", "inconclusive", "runtime-hog"),
        ],
        "seeds": [42],
    },
    "2": {
        "scenarios": [
            "lock-lapse-lone-node",
            "lock-stall-lone-node",
            "fence-token-at-write",
            "stress-ng-fs",
            "fsync-hard-outage",
            "fsync-soft-timeout",
            "fsync-interrupt",
            "kill9-remount",
            "stale-daemon-lock",
            "holder-kill-rejoin",
        ],
        "profiles": [
            "baseline",
            "cpu25",
            "cpu-bursty",
            "store-stall5",
            "store-stall20",
            "dm-delay",
            "hogs",
            "runtime-hog",
            "s3-jitter-high",
            "s3-blackhole",
            # Untargeted: the lock scenarios are excluded from these
            # (NO_UNTARGETED_PAUSE): the lone-node ones inject their own
            # precisely placed stall, which a second pause would break.
            "pause-takeover",
            "pause-lock-lapse",
            "step-pause",
            "clock-skew",
        ],
        "seeds": [42, 43, 44],
    },
    "3": {
        "scenarios": [
            "lock-holder-killed-contention",
            "lock-failover",
            "flock-cross-node",
            "lock-holder-partitioned",
            "lock-fence-at-close",
            "lock-release-at-close",
            "lock-grant-dead-generation",
            "git-under-flock*",
            "delegate-root-loss",
            "delegate-root-blackhole",
            "delegate-root-loss-ttl",
            "delegate-backup-handoff-failover",
            "root-failover-with-delegates",
            "delegate-handoff-renewal",
            "backup-failover*",
            "backup-departs",
            "backup-partition",
            "slow-s3-no-seal",
            "two-clients-shared",
            "p2p-partition-*",
            "fuse-inval-storm",
            "snapsched-write-overhead",
            "cto-strict*",
            "load-probe",
            "load-probe-takeover",
            "lock-contention-paused-holder",
            "lock-contention-stalled-owner",
            "lock-owner-reboot",
            "delegate-paused-across-ttl",
            "acks3-under-load",
            "snapsched-leader-paused",
            "endpoint-wedge-probe",
        ],
        "profiles": [
            "baseline",
            "cpu:25@a",
            "cpu25",
            "runtime-hog:4:200@b",
            "runtime-hog:4:200@a",
            "net-wan",
            "net-flap",
            "s3-jitter",
            "clock-skew",
            "step-pause",
        ],
        "seeds": [42, 43, 44],
    },
    # The takeover cells: load-probe-takeover mounts `b` first, so a pause
    # aimed at `b` deposes the root and `a` (its backup, the delegate of
    # its own directory) seals and takes over under load. The scenario
    # fails a cell where no takeover happened.
    "takeover": {
        "scenarios": ["load-probe-takeover"],
        "profiles": [
            "pause:lock-lapse+500:10000@b",
            "pause:takeover+300:15000@b",
            "step-pause:2000:8000@b",
        ],
        "seeds": [42, 43, 44, 55],
    },
    # Plan 43 C7a: the lock scenarios of phase 3 under oscillator rate
    # errors. Two nodes 1 000 ppm off in opposite directions (the budget as
    # it is: every window exact), and one node 5 % off with its budget
    # raised to cover it (`clock:rate:<ppm>` budgets the node unless
    # `:unbudgeted`).
    "clock-rate": {
        "scenarios": [
            "lock-contention-paused-holder",
            "git-under-flock-causal",
            "delegate-paused-across-ttl",
        ],
        "profiles": [
            "clock:rate:1000@a,clock:rate:-1000@b",
            "clock:rate:50000@a",
        ],
        "seeds": [42, 43, 44],
    },
    # Plan 43 C7b: offsets between the nodes' clocks far beyond the old
    # +-500 ms margin (the harness's cap is gone; one day is its sanity
    # bound). Every node 5 s ahead (nothing between the nodes, but the
    # wall against S3's and the harness's), a 5 s spread (a node 5 s ahead
    # beside one 5 s behind: 10 s apart, beyond the 3 s lock TTL some of
    # these scenarios run with), and node b a minute behind.
    "clock-offset": {
        "scenarios": [
            "lock-contention-paused-holder",
            "delegate-paused-across-ttl",
            "git-under-flock-causal",
            "visibility-s3-latency",
        ],
        "profiles": [
            "clock:5000",
            "clock:spread:5000",
            "clock:-60000@b",
        ],
        "seeds": [42, 43, 44],
    },
    "known-bugs": {
        "scenarios": ["stress-ng-fs-nodes", "stress-ng-fs-faults", "chaos-ci"],
        "profiles": ["baseline", "cpu25", "hogs", "runtime-hog", "net-flap", "s3-jitter"],
        "seeds": [42, 43, 44],
    },
}

DM_DELAY = "dm-delay"

# Scenarios whose exact asserts assume no lock holder (or delegate) is
# ever paused: a holder paused past its grant is fenced with EIO by
# contract, so an untargeted pause profile would fail them "correctly" and
# invite loosening the asserts. Their cells under such a profile are
# reported `excluded`; target a pause at a non-holder (`@node`) instead.
NO_UNTARGETED_PAUSE = [
    "lock-*",
    "fence-token-at-write",
    "flock-cross-node",
    "sqlite-two-nodes",
    "git-under-flock*",
    "delegate-*",
    "root-failover-with-delegates",
]


def untargeted_pause(profile: str) -> bool:
    """Whether a profile pauses every daemon (a pause item without `@node`,
    or a pause preset)."""
    for item in (i.strip() for i in profile.split(",")):
        if item.startswith(("pause", "step-pause")) and "@" not in item:
            return True
    return False


def slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._=+-]+", "_", s) or "baseline"


def harness_list(harness: str) -> list[str]:
    out = subprocess.run([harness, "list"], capture_output=True, text=True, check=True).stdout
    # Scenario rows are `<name> <desc>`; section headers end with a colon.
    return [
        line.split()[0]
        for line in out.splitlines()
        if line.strip() and not line[0].isspace() and not line.rstrip().endswith(":")
    ]


def expand(patterns: list[str], known: list[str]) -> list[str]:
    out: list[str] = []
    for p in patterns:
        hits = [k for k in known if fnmatch.fnmatchcase(k, p)]
        if not hits:
            sys.exit(f"run.py: no scenario matches {p!r} (harness list)")
        out += [h for h in hits if h not in out]
    return out


class Notice:
    """Lines in a shared notice file while a run injects load. Every change
    holds an exclusive `flock` on the file (writers that lock it too never
    lose a line to our rewrite)."""

    def __init__(self, path: str | None):
        self.path = path

    def _locked(self, edit) -> None:
        with open(self.path, "a+") as fh:
            fcntl.flock(fh, fcntl.LOCK_EX)
            try:
                fh.seek(0)
                lines = fh.read().splitlines()
                new = edit(lines)
                if new is not None:
                    fh.seek(0)
                    fh.truncate()
                    fh.write("".join(l + "\n" for l in new))
                    fh.flush()
            finally:
                fcntl.flock(fh, fcntl.LOCK_UN)

    def add(self, line: str) -> str | None:
        if not self.path:
            return None
        tagged = f"{line} [load-campaign pid {os.getpid()}]"
        self._locked(lambda lines: lines + [tagged])
        return tagged

    def remove(self, tagged: str | None) -> None:
        if not self.path or not tagged:
            return
        self._locked(lambda lines: [l for l in lines if l != tagged])


def inject(*args: str) -> str:
    # What it creates belongs to this runner (`inject.sh cleanup` removes
    # only a dead owner's, or ours).
    env = dict(os.environ, CST_LOAD_OWNER=str(os.getpid()))
    return subprocess.run(
        [str(HERE / "inject.sh"), *args], capture_output=True, text=True, check=True, env=env
    ).stdout


def foreign_notices(path: str | None) -> list[str]:
    """Lines of the shared notice file another run wrote (its load is on
    the host now)."""
    if not path:
        return []
    try:
        lines = Path(path).read_text().splitlines()
    except OSError:
        return []
    ours = f"[load-campaign pid {os.getpid()}]"
    return [l for l in lines if l.strip() and not l.endswith(ours)]


def run_cell(a, phase: str, profile: str, scenario: str, seed: int, brk: tuple[str, str] | None, notice: Notice) -> dict:
    items = [i.strip() for i in profile.split(",") if i.strip()]
    dm = DM_DELAY in items
    load = ",".join(i for i in items if i != DM_DELAY)
    name = scenario + (f"+break={brk[0]}" if brk else "")
    cell = Path(a.out) / slug(phase) / slug(profile) / slug(name) / f"seed-{seed}"
    cell.mkdir(parents=True, exist_ok=True)
    logs = cell / "logs"
    env = dict(os.environ)
    env.update(
        {
            "CONSTELLATION_HARNESS_VERIFY": "1",
            "CONSTELLATION_HARNESS_LOG_DIR": str(logs),
            "CONSTELLATION_HARNESS_HANG_S": str(a.hang),
            "CONSTELLATION_HARNESS_SCENARIO_TIMEOUT_S": str(a.timeout),
            "CONSTELLATION_HARNESS_DOCKER_PREFIX": a.docker_prefix,
            "HARNESS_KEEP_LOGS": "1",
            "CONSTELLATION_HARNESS_LOAD": load,
            # Liveness deadlines stretch under load; latency bounds have
            # the host-lag probe.
            "CONSTELLATION_HARNESS_DEADLINE_SCALE": "1" if profile in ("", "baseline") else str(a.deadline_scale),
        }
    )
    if a.bin:
        env["CONSTELLATION_BIN"] = a.bin
    if brk:
        env["CONSTELLATION_HARNESS_LOAD_PROBE_BREAK"] = brk[0]
        if brk[0] == "stall":
            env["CONSTELLATION_HARNESS_HANG_S"] = str(a.stall_hang)
    for kv in a.env:
        k, _, v = kv.partition("=")
        env[k] = v
    dmdir = None
    if dm:
        dmdir = cell / "dm-delay"
        rd, wr = a.dm_delay_ms.split(":")
        mnt = inject("dmdelay-up", str(dmdir), str(a.dm_delay_mib), rd, wr).strip().splitlines()[-1]
        env["TMPDIR"] = mnt
        env["CONSTELLATION_HARNESS_DISK_DIR"] = mnt
    cmd = [a.harness, "run", scenario, "--seed", str(seed), "--results-json", str(cell / "results.json")]
    started = time.time()
    tag = notice.add(
        f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} start: load profile {profile!r} on {name} seed {seed}"
    ) if profile not in ("", "baseline") else None
    print(f"--- {phase} | {profile} | {name} | seed {seed}", flush=True)
    status = None
    try:
        with open(cell / "harness.log", "w") as log:
            p = subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT, cwd=ROOT, start_new_session=True)
            try:
                status = p.wait(timeout=a.wall)
            except subprocess.TimeoutExpired:
                # The harness is its own process group: stop it by pid.
                os.killpg(p.pid, signal.SIGTERM)
                try:
                    p.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    os.killpg(p.pid, signal.SIGKILL)
                    p.wait()
                status = None
    finally:
        notice.remove(tag)
        if dmdir is not None:
            try:
                inject("dmdelay-down", str(dmdir))
            except subprocess.CalledProcessError as e:
                print(f"!!! dm-delay teardown: {e.stderr}", file=sys.stderr)
    res = {}
    try:
        res = json.loads((cell / "results.json").read_text())
    except (OSError, ValueError):
        pass
    sc = next((s for s in res.get("scenarios", []) if s.get("name") == scenario), None)
    if sc is None:
        verdict, reason = "harness-error", f"no result (harness exit {status}); see {cell / 'harness.log'}"
    else:
        verdict, reason = sc["outcome"], sc.get("reason")
    if brk:
        if brk[1] == "inconclusive":
            caught = verdict == "inconclusive"
        else:
            caught = verdict == "failed" and re.search(brk[1], reason or "") is not None
        verdict = "caught" if caught else "missed"
    row = {
        "phase": phase,
        "profile": profile,
        "scenario": name,
        "seed": seed,
        "verdict": verdict,
        "reason": reason,
        "seconds": round(time.time() - started, 1),
        "host_lag": (sc or {}).get("host_lag"),
        # `@node` items that matched no daemon: the cell did not run under
        # that part of its profile.
        "unapplied": (sc or {}).get("load_unapplied", []),
        "dir": str(cell),
    }
    (cell / "run.json").write_text(json.dumps(row, indent=2) + "\n")
    print(f"    {verdict}: {(reason or '')[:300]}", flush=True)
    if row["unapplied"]:
        print(f"    load applied to no daemon: {'; '.join(row['unapplied'])}", flush=True)
    return row


def worst_lag(h: dict | None) -> str:
    if not h:
        return ""
    probes = h.get("probes") or []
    if not probes:
        return ""
    w = max(probes, key=lambda p: p.get("stolen_ms", 0))
    psi = h.get("psi", {}).get("cpu", {}).get("some_pct")
    return f"{w['source']} p99 {w['p99_ms']:.1f} ms, max {w['max_ms']:.0f} ms, lost {w['stolen_ms']} ms; cpu psi {psi}%"


# More than this share of the judged cells ending `inconclusive` (a verdict that
# is no pass: the scenario's bounds could not be judged, or part of it was
# skipped) fails the campaign: a batch that proves nothing must not stay green.
MAX_INCONCLUSIVE_PCT = 10.0


def too_many_inconclusive(rows: list[dict], max_pct: float) -> str | None:
    judged = [r for r in rows if r["verdict"] not in ("excluded", "skipped")]
    inc = [r for r in judged if r["verdict"] == "inconclusive"]
    if judged and 100.0 * len(inc) / len(judged) > max_pct:
        return (f"{len(inc)} of {len(judged)} judged runs are inconclusive "
                f"({100.0 * len(inc) / len(judged):.0f}% > {max_pct:g}%)")
    return None


def summarize(out: Path, rows: list[dict] | None = None, max_inconclusive_pct: float = MAX_INCONCLUSIVE_PCT) -> int:
    if rows is None:
        rows = [json.loads(p.read_text()) for p in sorted(out.glob("**/run.json"))]
    (out / "summary.json").write_text(json.dumps(rows, indent=2) + "\n")
    bad = {"failed", "harness-error", "missed"}
    md = ["# Load campaign summary", ""]
    counts: dict[str, int] = {}
    for r in rows:
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    md.append(", ".join(f"{k}: {v}" for k, v in sorted(counts.items())) or "no runs")
    md += [
        "",
        "| Phase | Profile | Scenario | Seed | Verdict | s | Host lag | Load not applied | Reason |",
        "|---|---|---|---:|---|---:|---|---|---|",
    ]
    for r in rows:
        reason = (r.get("reason") or "").replace("|", "\\|").replace("\n", " ")[:240]
        md.append(
            f"| {r['phase']} | `{r['profile']}` | `{r['scenario']}` | {r['seed']} | **{r['verdict']}** "
            f"| {r['seconds']} | {worst_lag(r.get('host_lag'))} "
            f"| {'; '.join(r.get('unapplied') or []).replace('|', '/')} | {reason} |"
        )
    (out / "summary.md").write_text("\n".join(md) + "\n")
    print("\n".join(md[:3]))
    many = too_many_inconclusive(rows, max_inconclusive_pct)
    if many:
        print(f"!!! {many}: the batch proves too little", file=sys.stderr)
    return 1 if any(r["verdict"] in bad for r in rows) or many else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", help="output directory")
    ap.add_argument("--phase", choices=sorted(PHASES), help="a preset matrix")
    ap.add_argument("--profile", action="append", default=[], help="a load profile (repeatable; overrides the phase's)")
    ap.add_argument("--scenario", action="append", default=[], help="a scenario or glob (repeatable; overrides the phase's)")
    ap.add_argument("--seeds", help="comma-separated seeds (overrides the phase's)")
    ap.add_argument("--no-breaks", action="store_true", help="smoke: skip the deliberate-violation runs")
    ap.add_argument("--harness", default=os.environ.get("HARNESS_BIN", str(ROOT / "target/release/harness")))
    ap.add_argument("--bin", default=os.environ.get("CONSTELLATION_BIN"), help="the constellation binary")
    ap.add_argument("--timeout", type=int, default=1800, help="scenario timeout, s (the harness aborts it)")
    ap.add_argument("--wall", type=int, default=2400, help="kill the harness process after this, s")
    ap.add_argument("--hang", type=int, default=120, help="hang bound for one FUSE request, s")
    ap.add_argument("--stall-hang", type=int, default=6, help="hang bound of the smoke's stall break, s")
    ap.add_argument("--deadline-scale", type=float, default=3.0, help="liveness deadline factor under load")
    # The review's 500:2000 makes a fresh mount's metadata bootstrap take
    # tens of minutes (fsync-bound: 18 s at 5:20, over 6 min at 100:400).
    ap.add_argument("--dm-delay-ms", default="20:100", help="dm-delay read:write ms")
    ap.add_argument("--dm-delay-mib", type=int, default=4096)
    ap.add_argument("--docker-prefix", default=f"cst-load-{os.getpid()}", help="private container prefix")
    ap.add_argument("--notice", help="append a line here while a run injects load (shared hosts)")
    ap.add_argument("--env", action="append", default=[], help="K=V for every harness run (repeatable)")
    ap.add_argument("--summarize", metavar="DIR", help="only (re)write DIR's summary")
    ap.add_argument("--max-inconclusive-pct", type=float, default=MAX_INCONCLUSIVE_PCT,
                    help="fail when more than this share of the judged runs is inconclusive")
    a = ap.parse_args()
    if a.summarize:
        return summarize(Path(a.summarize), None, a.max_inconclusive_pct)
    if not a.out or not (a.phase or a.scenario):
        ap.error("--out and --phase or --scenario are required")
    preset = PHASES.get(a.phase or "", {})
    known = harness_list(a.harness)
    scenarios = expand(a.scenario or preset.get("scenarios", []), known)
    profiles = a.profile or preset.get("profiles", ["baseline"])
    seeds = [int(s) for s in a.seeds.split(",")] if a.seeds else preset.get("seeds", [42])
    phase = a.phase or "adhoc"
    Path(a.out).mkdir(parents=True, exist_ok=True)
    notice = Notice(a.notice)
    rows = []
    try:
        for profile in profiles:
            for scenario in scenarios:
                for seed in seeds:
                    if untargeted_pause(profile) and any(
                        fnmatch.fnmatchcase(scenario, p) for p in NO_UNTARGETED_PAUSE
                    ):
                        print(f"--- {phase} | {profile} | {scenario} | seed {seed}: excluded "
                              "(asserts an unpaused holder; target the pause at a non-holder)")
                        rows.append({
                            "phase": phase, "profile": profile, "scenario": scenario,
                            "seed": seed, "verdict": "excluded",
                            "reason": "untargeted pause profile x a scenario asserting no EIO "
                                      "for an unpaused holder",
                            "seconds": 0, "host_lag": None, "dir": "",
                        })
                        continue
                    rows.append(run_cell(a, phase, profile, scenario, seed, None, notice))
        if not a.no_breaks and not a.profile and not a.scenario:
            for name, expect, profile in preset.get("breaks", []):
                rows.append(run_cell(a, phase, profile, "load-probe", seeds[0], (name, expect), notice))
    finally:
        # Only what this run (or a dead one) left; and not at all while
        # another run's load is on the host by the notice file.
        foreign = foreign_notices(a.notice)
        if foreign:
            print(f"--- cleanup skipped: another run's load is on: {foreign[0]}", file=sys.stderr)
        else:
            try:
                print(inject("cleanup"), end="")
            except subprocess.CalledProcessError as e:
                print(f"!!! cleanup: {e.stderr}", file=sys.stderr)
    return summarize(Path(a.out), rows, a.max_inconclusive_pct)


if __name__ == "__main__":
    sys.exit(main())
