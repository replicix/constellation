#!/usr/bin/env python3
"""Platform parity checker (plan 31 C6, DESIGN in plan 31 section 8/12).

    python3 tests/parity.py --expect tests/platform-parity.toml \
        [--require-lane LANE ...] [--absent-lane LANE ...] results/results-*.json

Inputs are the `--results-json` files of `harness run` (schema 1, see
crates/harness/src/results.rs). Files that share a `lane` are shards of one
run and are merged by concatenating their `scenarios`; a scenario name that
appears twice in one lane is an error (overlapping shards, or one file given
twice).

One lane is the reference (default `linux-fuse`; the expectations file may
set a top-level `reference = "..."`, and `[lane."<name>"] reference = "..."`
gives one lane its own reference, so plans 34/35/36 can add lanes without
touching this checker). For every other lane and every scenario:

  * the outcome must equal the reference lane's outcome, unless an
    `[[expect]]` entry covers that (scenario, lane);
  * a scenario present in one lane of a pair and missing from the other is
    a violation (a lost shard must not look like a pass);
  * any `failed` outcome, in any lane including the reference, is a
    violation, and `failed` can never be expected;
  * every `--require-lane` must have results (CI names the lanes it ran, so
    a lane whose job died before writing its results file cannot drop out
    of the comparison unnoticed);
  * an `--absent-lane` is one this run deliberately did not produce (the
    `linux-csi` lane needs a FUSE-capable kind runner that a workflow may
    not have): its `[[expect]]` entries are not checked, the summary says
    it was not run, and results for it are a violation. A lane cannot be
    both required and absent.

Expectations are two-way, like the xfstests baseline: an `[[expect]]` entry
that no longer matches reality fails the check too. That covers a lane or
scenario that is not in the results, a lane outcome different from the one
the entry states, and an entry whose lane outcome now equals the reference
(nothing left to excuse).

`[[expect]]` shape:

    [[expect]]
    scenario = "<name>"           # or "*" (capability skips only)
    lanes    = ["<os>-<frontend>", ...]
    outcome  = "skipped"          # the only outcome that may differ
    cap      = "<Cap>"            # required with scenario = "*"
    reason   = "<why this lane legitimately differs>"

A wildcard entry covers every scenario of the listed lanes that is skipped
there while the reference lane did not skip it, and whose recorded skip
reason names the `cap` (as a whole word). The cap binding is what keeps a
wildcard from also excusing an unrelated skip; a missing-tool skip (the
harness's "<tool> not installed") is never a capability skip, so no
wildcard covers it, whatever its `cap`.

Output is a Markdown summary (for $GITHUB_STEP_SUMMARY) on stdout. Exit
status: 0 = parity holds, 1 = violations, 2 = unusable input or expectations
file.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from dataclasses import dataclass, field

SCHEMA = 1
DEFAULT_REFERENCE = "linux-fuse"
OUTCOMES = ("passed", "failed", "skipped")
ENTRY_KEYS = {"scenario", "lanes", "outcome", "cap", "reason"}
TOP_KEYS = {"reference", "lane", "expect"}


class ParityError(Exception):
    """Unusable input (results or expectations); the check cannot run."""


@dataclass(frozen=True)
class Result:
    outcome: str
    reason: str | None = None


# lane -> scenario name -> Result
Lanes = dict[str, dict[str, Result]]


@dataclass(frozen=True)
class Expect:
    scenario: str
    lanes: tuple[str, ...]
    outcome: str
    reason: str
    cap: str | None = None

    @property
    def wildcard(self) -> bool:
        return self.scenario == "*"

    def label(self) -> str:
        cap = f" cap={self.cap}" if self.cap else ""
        return f"scenario={self.scenario!r} lanes={list(self.lanes)}{cap}"


@dataclass
class Config:
    reference: str = DEFAULT_REFERENCE
    lane_reference: dict[str, str] = field(default_factory=dict)
    expects: list[Expect] = field(default_factory=list)

    def reference_of(self, lane: str) -> str:
        return self.lane_reference.get(lane, self.reference)


@dataclass(frozen=True)
class Violation:
    kind: str
    lane: str
    scenario: str
    detail: str


@dataclass(frozen=True)
class Covered:
    scenario: str
    lane: str
    reference: str
    outcome: str
    why: str


@dataclass
class Report:
    lanes: Lanes
    config: Config
    absent: tuple[str, ...] = ()
    violations: list[Violation] = field(default_factory=list)
    covered: list[Covered] = field(default_factory=list)
    # (scenario, lane, reference outcome, lane outcome) for the diff table
    differences: list[tuple[str, str, str, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations


# --------------------------------------------------------------------------
# Loading


def load_results(paths: list[str]) -> Lanes:
    """Read results files, merge shards per lane, reject duplicates."""
    lanes: Lanes = {}
    if not paths:
        raise ParityError("no results files given")
    for path in paths:
        try:
            with open(path, "rb") as fh:
                doc = json.load(fh)
        except (OSError, ValueError) as e:
            raise ParityError(f"{path}: cannot read results: {e}") from e
        merge_results(lanes, doc, path)
    return lanes


def merge_results(lanes: Lanes, doc: object, source: str = "<results>") -> None:
    if not isinstance(doc, dict):
        raise ParityError(f"{source}: top level is not an object")
    if doc.get("schema") != SCHEMA:
        raise ParityError(f"{source}: unsupported schema {doc.get('schema')!r} (want {SCHEMA})")
    lane = doc.get("lane")
    if not isinstance(lane, str) or not lane:
        raise ParityError(f"{source}: missing `lane`")
    scenarios = doc.get("scenarios")
    if not isinstance(scenarios, list):
        raise ParityError(f"{source}: `scenarios` is not a list")
    bucket = lanes.setdefault(lane, {})
    for i, sc in enumerate(scenarios):
        if not isinstance(sc, dict):
            raise ParityError(f"{source}: scenarios[{i}] is not an object")
        name, outcome = sc.get("name"), sc.get("outcome")
        if not isinstance(name, str) or not name:
            raise ParityError(f"{source}: scenarios[{i}] has no `name`")
        if outcome not in OUTCOMES:
            raise ParityError(f"{source}: scenario {name!r} has outcome {outcome!r}")
        if name in bucket:
            raise ParityError(
                f"{source}: scenario {name!r} appears twice in lane {lane!r} "
                "(overlapping shards, or the same results file given twice)"
            )
        reason = sc.get("reason")
        bucket[name] = Result(outcome, reason if isinstance(reason, str) else None)


def load_config(path: str | None) -> Config:
    if path is None:
        return Config()
    try:
        with open(path, "rb") as fh:
            doc = tomllib.load(fh)
    except (OSError, tomllib.TOMLDecodeError) as e:
        raise ParityError(f"{path}: cannot read expectations: {e}") from e
    return parse_config(doc, path)


def parse_config(doc: dict, source: str = "<expectations>") -> Config:
    unknown = set(doc) - TOP_KEYS
    if unknown:
        raise ParityError(f"{source}: unknown top-level key(s) {sorted(unknown)}")
    cfg = Config()
    ref = doc.get("reference", DEFAULT_REFERENCE)
    if not isinstance(ref, str) or not ref:
        raise ParityError(f"{source}: `reference` must be a lane name")
    cfg.reference = ref
    for lane, tbl in (doc.get("lane") or {}).items():
        if not isinstance(tbl, dict) or set(tbl) - {"reference"}:
            raise ParityError(f"{source}: [lane.{lane!r}] only supports `reference`")
        lref = tbl.get("reference")
        if not isinstance(lref, str) or not lref:
            raise ParityError(f"{source}: [lane.{lane!r}] needs a `reference` lane name")
        if lref == lane:
            raise ParityError(f"{source}: lane {lane!r} cannot be its own reference")
        cfg.lane_reference[lane] = lref
    entries = doc.get("expect", [])
    if not isinstance(entries, list):
        raise ParityError(f"{source}: `expect` must be an array of tables ([[expect]])")
    for i, raw in enumerate(entries):
        cfg.expects.append(parse_expect(raw, f"{source}: [[expect]] #{i + 1}"))
    return cfg


def parse_expect(raw: object, where: str) -> Expect:
    if not isinstance(raw, dict):
        raise ParityError(f"{where}: not a table")
    unknown = set(raw) - ENTRY_KEYS
    if unknown:
        raise ParityError(f"{where}: unknown key(s) {sorted(unknown)}")
    scenario = raw.get("scenario")
    if not isinstance(scenario, str) or not scenario:
        raise ParityError(f"{where}: `scenario` is required")
    lanes = raw.get("lanes")
    if (
        not isinstance(lanes, list)
        or not lanes
        or not all(isinstance(x, str) and x for x in lanes)
    ):
        raise ParityError(f"{where}: `lanes` must be a non-empty list of lane names")
    outcome = raw.get("outcome")
    if outcome == "failed":
        raise ParityError(f"{where}: `failed` can never be expected")
    if outcome != "skipped":
        raise ParityError(
            f"{where}: `outcome` must be \"skipped\" (only skips may differ from the "
            f"reference lane), got {outcome!r}"
        )
    reason = raw.get("reason")
    if not isinstance(reason, str) or not reason.strip():
        raise ParityError(f"{where}: a `reason` is required (why the lane legitimately differs)")
    cap = raw.get("cap")
    if cap is not None and (not isinstance(cap, str) or not cap.strip()):
        raise ParityError(f"{where}: `cap` must be a non-empty string")
    if scenario == "*" and cap is None:
        raise ParityError(f"{where}: wildcard scenario = \"*\" requires `cap = \"...\"`")
    return Expect(scenario, tuple(lanes), outcome, reason.strip(), cap)


# --------------------------------------------------------------------------
# Checking


def check(
    lanes: Lanes,
    cfg: Config,
    required: list[str] | tuple[str, ...] = (),
    absent: list[str] | tuple[str, ...] = (),
) -> Report:
    both = sorted(set(required) & set(absent))
    if both:
        raise ParityError(f"lane(s) {both} both required and absent")
    rep = Report(lanes, cfg, tuple(sorted(set(absent))))
    add = rep.violations.append

    for lane in rep.absent:
        if lane in lanes:
            add(
                Violation(
                    "absent-lane",
                    lane,
                    "*",
                    "declared absent (--absent-lane) but has results; check it or drop the flag",
                )
            )

    for lane in sorted(set(required)):
        if lane not in lanes:
            add(
                Violation(
                    "missing-lane",
                    lane,
                    "*",
                    f"required lane has no results (lanes present: {sorted(lanes)})",
                )
            )

    # Any failure is a violation, in every lane, whatever the reference says.
    for lane in sorted(lanes):
        for name in sorted(lanes[lane]):
            r = lanes[lane][name]
            if r.outcome == "failed":
                add(Violation("failed", lane, name, r.reason or "scenario failed"))

    # Which references are needed, and are they all present?
    for lane in sorted(lanes):
        ref = cfg.reference_of(lane)
        if lane != ref and ref not in lanes:
            add(
                Violation(
                    "missing-reference",
                    lane,
                    "*",
                    f"reference lane {ref!r} has no results (lanes present: {sorted(lanes)})",
                )
            )
    if cfg.reference not in lanes:
        add(
            Violation(
                "missing-reference",
                cfg.reference,
                "*",
                f"reference lane {cfg.reference!r} has no results (lanes present: {sorted(lanes)})",
            )
        )

    used: set[tuple[int, str]] = set()
    for lane in sorted(lanes):
        ref = cfg.reference_of(lane)
        if lane == ref or ref not in lanes:
            continue
        ref_res, res = lanes[ref], lanes[lane]
        for name in sorted(set(ref_res) | set(res)):
            if name not in res:
                add(Violation("missing", lane, name, f"present in {ref!r}, absent from this lane"))
                continue
            if name not in ref_res:
                add(Violation("missing", ref, name, f"present in {lane!r}, absent from {ref!r}"))
                continue
            got, want = res[name], ref_res[name]
            if got.outcome == want.outcome:
                continue
            rep.differences.append((name, lane, want.outcome, got.outcome))
            if got.outcome == "failed" or want.outcome == "failed":
                continue  # already reported as `failed`
            idx = covering(cfg, name, lane, got, want)
            if idx is None:
                add(
                    Violation(
                        "mismatch",
                        lane,
                        name,
                        f"{ref}={want.outcome}, {lane}={got.outcome}"
                        + (f" ({got.reason})" if got.reason else "")
                        + "; no [[expect]] entry covers it",
                    )
                )
            else:
                used.add((idx, lane))
                rep.covered.append(
                    Covered(name, lane, want.outcome, got.outcome, cfg.expects[idx].reason)
                )

    stale_check(rep, lanes, cfg, used)
    return rep


def covering(cfg: Config, name: str, lane: str, got: Result, want: Result) -> int | None:
    """Index of the first expectation excusing this difference, if any."""
    if got.outcome != "skipped" or want.outcome == "skipped":
        return None
    for i, e in enumerate(cfg.expects):
        if lane not in e.lanes:
            continue
        if e.wildcard and e.cap and is_cap_skip(got.reason, e.cap):
            return i
        if not e.wildcard and e.scenario == name:
            return i
    return None


TOOL_SKIP = " not installed"


def is_cap_skip(reason: str | None, cap: str) -> bool:
    """Whether a skip `reason` is a capability skip naming `cap`."""
    if not reason or reason.endswith(TOOL_SKIP):
        return False
    return re.search(rf"(?<![\w-]){re.escape(cap)}(?![\w-])", reason) is not None


def stale_check(rep: Report, lanes: Lanes, cfg: Config, used: set[tuple[int, str]]) -> None:
    add = rep.violations.append
    for i, e in enumerate(cfg.expects):
        for lane in e.lanes:
            if lane in rep.absent:
                continue
            if lane not in lanes:
                add(Violation("stale-expect", lane, e.scenario, f"lane not in results ({e.label()})"))
            elif lane == cfg.reference_of(lane):
                add(
                    Violation(
                        "stale-expect",
                        lane,
                        e.scenario,
                        f"lane is a reference lane; nothing to expect ({e.label()})",
                    )
                )
            elif not e.wildcard:
                got = lanes[lane].get(e.scenario)
                ref = lanes.get(cfg.reference_of(lane), {}).get(e.scenario)
                if got is None:
                    add(
                        Violation(
                            "stale-expect",
                            lane,
                            e.scenario,
                            f"scenario not in this lane's results ({e.label()})",
                        )
                    )
                elif got.outcome != e.outcome:
                    add(
                        Violation(
                            "stale-expect",
                            lane,
                            e.scenario,
                            f"entry says {e.outcome!r} but the lane reports {got.outcome!r}",
                        )
                    )
                elif ref is not None and ref.outcome == got.outcome:
                    add(
                        Violation(
                            "stale-expect",
                            lane,
                            e.scenario,
                            f"lane no longer differs from the reference ({got.outcome!r}); "
                            "remove the entry",
                        )
                    )
            elif e.wildcard and (i, lane) not in used:
                add(
                    Violation(
                        "stale-expect",
                        lane,
                        "*",
                        f"wildcard covers no skipped scenario whose reason mentions "
                        f"{e.cap!r} ({e.label()})",
                    )
                )


# --------------------------------------------------------------------------
# Rendering


def md(s: str) -> str:
    return s.replace("|", "\\|").replace("\n", " ")


def render(rep: Report) -> str:
    cfg = rep.config
    out = ["## Platform parity", ""]
    out.append(
        f"Reference lane: `{cfg.reference}`. Verdict: **{'PASS' if rep.ok else 'FAIL'}**"
        + ("" if rep.ok else f" ({len(rep.violations)} violation(s))")
    )
    out += ["", "| Lane | Reference | Scenarios | passed | skipped | failed | Differs from reference |"]
    out.append("|---|---|---:|---:|---:|---:|---:|")
    diffs_by_lane: dict[str, int] = {}
    for _, lane, _, _ in rep.differences:
        diffs_by_lane[lane] = diffs_by_lane.get(lane, 0) + 1
    for lane in sorted(rep.lanes):
        res = rep.lanes[lane]
        n = {o: sum(1 for r in res.values() if r.outcome == o) for o in OUTCOMES}
        ref = cfg.reference_of(lane)
        out.append(
            f"| `{lane}` | {'(reference)' if ref == lane else '`' + ref + '`'} | {len(res)} "
            f"| {n['passed']} | {n['skipped']} | {n['failed']} | {diffs_by_lane.get(lane, 0)} |"
        )
    for lane in rep.absent:
        if lane not in rep.lanes:
            out.append(f"| `{lane}` | not run (`--absent-lane`) | | | | | |")
    if rep.violations:
        out += ["", "### Violations", "", "| Kind | Lane | Scenario | Detail |", "|---|---|---|---|"]
        for v in rep.violations:
            out.append(f"| {v.kind} | `{md(v.lane)}` | `{md(v.scenario)}` | {md(v.detail)} |")
    if rep.covered:
        out += [
            "",
            "### Expected differences",
            "",
            "| Scenario | Lane | Reference | Lane | Why |",
            "|---|---|---|---|---|",
        ]
        for c in rep.covered:
            out.append(
                f"| `{md(c.scenario)}` | `{c.lane}` | {c.reference} | {c.outcome} | {md(c.why)} |"
            )
    if rep.ok and not rep.covered:
        out += ["", "Every lane matches the reference on every scenario."]
    return "\n".join(out) + "\n"


def render_error(msg: str) -> str:
    return f"## Platform parity\n\nVerdict: **FAIL** (unusable input)\n\n{msg}\n"


# --------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Check that every harness lane matches the reference lane.",
        epilog="Exit: 0 parity holds, 1 violations, 2 unusable input.",
    )
    ap.add_argument("--expect", metavar="TOML", help="expectations file (tests/platform-parity.toml)")
    ap.add_argument(
        "--require-lane",
        metavar="LANE",
        action="append",
        default=[],
        help="a lane that must have results (repeatable); a missing one is a violation",
    )
    ap.add_argument(
        "--absent-lane",
        metavar="LANE",
        action="append",
        default=[],
        help="a lane deliberately not run (repeatable): its expectations are not checked, "
        "and results for it are a violation",
    )
    ap.add_argument("results", nargs="+", metavar="results.json", help="harness --results-json files")
    args = ap.parse_args(argv)
    try:
        cfg = load_config(args.expect)
        lanes = load_results(args.results)
        rep = check(lanes, cfg, args.require_lane, args.absent_lane)
    except ParityError as e:
        print(render_error(str(e)))
        print(f"parity: {e}", file=sys.stderr)
        return 2
    print(render(rep))
    if not rep.ok:
        for v in rep.violations:
            print(f"parity: {v.kind}: {v.lane}: {v.scenario}: {v.detail}", file=sys.stderr)
    return 0 if rep.ok else 1


if __name__ == "__main__":
    sys.exit(main())
