"""Unit tests for tests/parity.py against synthetic results sets.

    python3 -m unittest discover -s tests -p 'test_parity.py' -v

The point of the suite is the fail-closed direction: a deliberate mismatch
must make the checker exit nonzero (plan 31 C6 gate), and an expectation
that stopped matching reality must fail too.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import tomllib
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parity  # noqa: E402

SCEN = ["basic-rw", "crash-heal", "fio-latency"]


def results(lane, outcomes, shard=None, **extra):
    """A schema-1 results document. `outcomes` maps name -> outcome or
    (outcome, reason)."""
    scenarios = []
    for name, o in outcomes.items():
        outcome, reason = o if isinstance(o, tuple) else (o, None)
        scenarios.append(
            {"name": name, "outcome": outcome, "seconds": 1.0, "reason": reason}
        )
    return {
        "schema": 1,
        "lane": lane,
        "seed": 42,
        "shard": shard,
        "started_at": 1790000000,
        "scenarios": scenarios,
        **extra,
    }


def all_passed(lane, names=SCEN, **kw):
    return results(lane, {n: "passed" for n in names}, **kw)


class Run:
    """Write docs (+ optional TOML text) to files and run parity.main()."""

    def __init__(self, docs, toml=None):
        self.tmp = tempfile.TemporaryDirectory()
        self.paths = []
        for i, doc in enumerate(docs):
            p = os.path.join(self.tmp.name, f"results-{i}.json")
            with open(p, "w") as fh:
                json.dump(doc, fh)
            self.paths.append(p)
        self.args = list(self.paths)
        if toml is not None:
            tp = os.path.join(self.tmp.name, "parity.toml")
            with open(tp, "w") as fh:
                fh.write(toml)
            self.args = ["--expect", tp] + self.args

    def __enter__(self):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            self.code = parity.main(self.args)
        self.stdout, self.stderr = out.getvalue(), err.getvalue()
        return self

    def __exit__(self, *exc):
        self.tmp.cleanup()


def expect(scenario="fio-latency", lanes=("linux-fuse-process",), **kw):
    fields = {"outcome": "skipped", "reason": "test", **kw}
    body = [f'scenario = "{scenario}"', f"lanes = {json.dumps(list(lanes))}"]
    for k, v in fields.items():
        body.append(f"{k} = {json.dumps(v)}")
    return "[[expect]]\n" + "\n".join(body) + "\n"


class ParityTests(unittest.TestCase):
    def test_all_equal_passes(self):
        docs = [all_passed("linux-fuse"), all_passed("linux-fuse-process")]
        with Run(docs) as r:
            self.assertEqual(r.code, 0, r.stderr)
        self.assertIn("**PASS**", r.stdout)
        self.assertIn("| `linux-fuse-process` |", r.stdout)

    def test_seeded_file_parses_and_is_empty(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "platform-parity.toml")
        cfg = parity.load_config(path)
        self.assertEqual(cfg.reference, "linux-fuse")
        self.assertEqual(cfg.expects, [])
        docs = [all_passed("linux-fuse"), all_passed("linux-fuse-process")]
        with Run(docs) as r:
            self.assertEqual(r.code, 0)
        # The real file, through the CLI.
        with tempfile.TemporaryDirectory() as d:
            paths = []
            for i, doc in enumerate(docs):
                paths.append(os.path.join(d, f"r{i}.json"))
                with open(paths[-1], "w") as fh:
                    json.dump(doc, fh)
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(parity.main(["--expect", path, *paths]), 0)

    def test_deliberate_mismatch_fails_closed(self):
        lane = all_passed("linux-fuse-process")
        lane["scenarios"][1]["outcome"] = "skipped"
        lane["scenarios"][1]["reason"] = "flaky backend"
        with Run([all_passed("linux-fuse"), lane], toml="") as r:
            self.assertNotEqual(r.code, 0)
        self.assertEqual(r.code, 1)
        self.assertIn("mismatch", r.stdout)
        self.assertIn("crash-heal", r.stdout)
        self.assertIn("**FAIL**", r.stdout)

    def test_mismatch_without_any_expect_file_fails(self):
        lane = results("linux-fuse-process", {"basic-rw": "skipped"})
        with Run([results("linux-fuse", {"basic-rw": "passed"}), lane]) as r:
            self.assertEqual(r.code, 1)

    def test_covered_skip_passes(self):
        ref = all_passed("linux-fuse")
        lane = all_passed("linux-fuse-process")
        lane["scenarios"][2].update(outcome="skipped", reason="no toxiproxy latency")
        with Run([ref, lane], toml=expect(reason="native proxy cannot do this")) as r:
            self.assertEqual(r.code, 0, r.stderr + r.stdout)
        self.assertIn("Expected differences", r.stdout)
        self.assertIn("native proxy cannot do this", r.stdout)

    def test_expect_only_covers_its_own_lane_and_scenario(self):
        ref = all_passed("linux-fuse")
        lane = all_passed("linux-fuse-process")
        lane["scenarios"][2].update(outcome="skipped", reason="x")
        lane["scenarios"][0].update(outcome="skipped", reason="y")
        with Run([ref, lane], toml=expect()) as r:  # covers fio-latency only
            self.assertEqual(r.code, 1)
        self.assertIn("`basic-rw`", r.stdout)
        other = expect(lanes=("some-other-lane",))
        lane = all_passed("linux-fuse-process")
        lane["scenarios"][2].update(outcome="skipped", reason="x")
        with Run([ref, lane, all_passed("some-other-lane")], toml=other) as r:
            self.assertEqual(r.code, 1)

    def test_stale_expectation_lane_now_matches_reference(self):
        docs = [all_passed("linux-fuse"), all_passed("linux-fuse-process")]
        with Run(docs, toml=expect()) as r:
            self.assertEqual(r.code, 1)
        self.assertIn("stale-expect", r.stdout)

    def test_stale_expectation_lane_outcome_differs_from_entry(self):
        # Entry says skipped, but the lane failed to skip and actually passed
        # while the reference skipped: a real difference, not the stated one.
        ref = all_passed("linux-fuse")
        ref["scenarios"][2].update(outcome="skipped", reason="no fio")
        with Run([ref, all_passed("linux-fuse-process")], toml=expect()) as r:
            self.assertEqual(r.code, 1)
        self.assertIn("stale-expect", r.stdout)
        self.assertIn("mismatch", r.stdout)

    def test_stale_expectation_unknown_scenario_or_lane(self):
        docs = [all_passed("linux-fuse"), all_passed("linux-fuse-process")]
        with Run(docs, toml=expect(scenario="no-such-scenario")) as r:
            self.assertEqual(r.code, 1)
        self.assertIn("scenario not in this lane", r.stdout)
        with Run(docs, toml=expect(lanes=("macos-nfs",))) as r:
            self.assertEqual(r.code, 1)
        self.assertIn("lane not in results", r.stdout)

    def test_wildcard_without_cap_is_rejected(self):
        docs = [all_passed("linux-fuse"), all_passed("linux-fuse-process")]
        with Run(docs, toml=expect(scenario="*")) as r:
            self.assertEqual(r.code, 2)
        self.assertIn("requires `cap", r.stdout)

    def test_wildcard_with_cap_covers_only_that_cap(self):
        ref = all_passed("linux-fuse")
        lane = all_passed("linux-fuse-process")
        lane["scenarios"][0].update(outcome="skipped", reason="cap Symlinks not supported")
        lane["scenarios"][1].update(outcome="skipped", reason="cap Symlinks not supported")
        toml = expect(scenario="*", cap="Symlinks")
        with Run([ref, lane], toml=toml) as r:
            self.assertEqual(r.code, 0, r.stdout)
        # An unrelated skip is not excused by the wildcard.
        lane["scenarios"][2].update(outcome="skipped", reason="fio not installed")
        with Run([ref, lane], toml=toml) as r:
            self.assertEqual(r.code, 1)
        self.assertIn("`fio-latency`", r.stdout)

    def test_wildcard_never_covers_tool_skips_or_partial_names(self):
        ref = all_passed("linux-fuse")
        lane = all_passed("linux-fuse-process")
        # A tool skip that happens to contain the cap's name.
        lane["scenarios"][2].update(outcome="skipped", reason="fio not installed")
        with Run([ref, lane], toml=expect(scenario="*", cap="fio")) as r:
            self.assertEqual(r.code, 1)
        self.assertIn("| mismatch | `linux-fuse-process` | `fio-latency` |", r.stdout)
        # The cap must be named as a whole word, not as part of another one.
        lane["scenarios"][2].update(reason="cap SymlinksToDirs not supported")
        with Run([ref, lane], toml=expect(scenario="*", cap="Symlinks")) as r:
            self.assertEqual(r.code, 1)
        self.assertIn("| mismatch | `linux-fuse-process` | `fio-latency` |", r.stdout)

    def test_wildcard_that_matches_nothing_is_stale(self):
        docs = [all_passed("linux-fuse"), all_passed("linux-fuse-process")]
        with Run(docs, toml=expect(scenario="*", cap="Symlinks")) as r:
            self.assertEqual(r.code, 1)
        self.assertIn("wildcard covers no skipped scenario", r.stdout)

    def test_failed_expected_is_rejected(self):
        docs = [all_passed("linux-fuse"), all_passed("linux-fuse-process")]
        with Run(docs, toml=expect(outcome="failed")) as r:
            self.assertEqual(r.code, 2)
        self.assertIn("`failed` can never be expected", r.stdout)

    def test_only_skipped_may_be_expected(self):
        with self.assertRaises(parity.ParityError):
            parity.parse_config(tomllib.loads(expect(outcome="passed")))

    def test_expect_needs_reason_and_rejects_unknown_keys(self):
        for text in (
            '[[expect]]\nscenario = "a"\nlanes = ["x"]\noutcome = "skipped"\n',
            expect(bogus=1),
            '[[expect]]\nscenario = "a"\nlanes = []\noutcome = "skipped"\nreason = "r"\n',
        ):
            with self.assertRaises(parity.ParityError, msg=text):
                parity.parse_config(tomllib.loads(text))
        with self.assertRaises(parity.ParityError):
            parity.parse_config({"surprise": 1})

    def test_failed_outcome_fails_in_any_lane_even_if_equal(self):
        ref = all_passed("linux-fuse")
        ref["scenarios"][0].update(outcome="failed", reason="boom")
        lane = all_passed("linux-fuse-process")
        lane["scenarios"][0].update(outcome="failed", reason="boom")
        with Run([ref, lane]) as r:
            self.assertEqual(r.code, 1)
        self.assertIn("| failed | `linux-fuse` | `basic-rw` |", r.stdout)
        self.assertIn("| failed | `linux-fuse-process` | `basic-rw` |", r.stdout)

    def test_failed_only_in_reference_fails(self):
        ref = all_passed("linux-fuse")
        ref["scenarios"][1].update(outcome="failed", reason="boom")
        with Run([ref]) as r:  # even a single-lane run
            self.assertEqual(r.code, 1)

    def test_shards_are_merged(self):
        s1 = results("linux-fuse", {"basic-rw": "passed", "fio-latency": "passed"}, shard="1/2")
        s2 = results("linux-fuse", {"crash-heal": "passed"}, shard="2/2")
        lane = all_passed("linux-fuse-process")
        with Run([s1, s2, lane]) as r:
            self.assertEqual(r.code, 0, r.stdout)
        self.assertIn("| `linux-fuse` | (reference) | 3 |", r.stdout)

    def test_lost_shard_is_reported_as_missing(self):
        s1 = results("linux-fuse-process", {"basic-rw": "passed", "fio-latency": "passed"}, shard="1/2")
        with Run([all_passed("linux-fuse"), s1]) as r:
            self.assertEqual(r.code, 1)
        self.assertIn("| missing | `linux-fuse-process` | `crash-heal` |", r.stdout)

    def test_scenario_only_in_one_lane_is_reported_both_ways(self):
        ref = all_passed("linux-fuse", ["basic-rw"])
        lane = all_passed("linux-fuse-process", ["basic-rw", "extra"])
        with Run([ref, lane]) as r:
            self.assertEqual(r.code, 1)
        self.assertIn("`extra`", r.stdout)
        self.assertIn("absent from 'linux-fuse'", r.stdout)

    def test_duplicate_scenario_within_lane_is_an_error(self):
        s1 = results("linux-fuse", {"basic-rw": "passed"}, shard="1/2")
        s2 = results("linux-fuse", {"basic-rw": "passed"}, shard="2/2")
        with Run([s1, s2]) as r:
            self.assertEqual(r.code, 2)
        self.assertIn("appears twice", r.stdout)

    def test_same_file_twice_is_a_duplicate(self):
        doc = all_passed("linux-fuse")
        with Run([doc]) as r:
            pass
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            code = parity.main(r.paths * 2)
        self.assertEqual(code, 2)

    def test_missing_reference_lane_fails(self):
        with Run([all_passed("linux-fuse-process"), all_passed("macos-nfs")]) as r:
            self.assertEqual(r.code, 1)
        self.assertIn("missing-reference", r.stdout)
        self.assertIn("reference lane 'linux-fuse' has no results", r.stdout)

    def test_required_lane_without_results_fails(self):
        # The process lane's job died before writing its results: the
        # reference alone must not pass as parity.
        with Run([all_passed("linux-fuse")]) as r:
            self.assertEqual(r.code, 0)
        args = ["--require-lane", "linux-fuse", "--require-lane", "linux-fuse-process"]
        with Run([all_passed("linux-fuse")]) as r:
            out = io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
                code = parity.main(args + r.paths)
        self.assertEqual(code, 1)
        self.assertIn("| missing-lane | `linux-fuse-process` |", out.getvalue())
        with Run([all_passed("linux-fuse"), all_passed("linux-fuse-process")]) as r:
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(parity.main(args + r.paths), 0)

    def test_many_lanes_each_against_reference(self):
        ref = all_passed("linux-fuse")
        good = all_passed("linux-fuse-process")
        nfs = all_passed("macos-nfs")
        nfs["scenarios"][2].update(outcome="skipped", reason="cap Xattrs missing")
        toml = expect(lanes=("macos-nfs",), reason="NFS has no such thing")
        with Run([ref, good, nfs], toml=toml) as r:
            self.assertEqual(r.code, 0, r.stdout)
        bad = all_passed("windows-winfsp")
        bad["scenarios"][0]["outcome"] = "skipped"
        with Run([ref, good, nfs, bad], toml=toml) as r:
            self.assertEqual(r.code, 1)
        self.assertIn("`windows-winfsp`", r.stdout)

    def test_per_lane_reference(self):
        toml = '[lane."linux-nfs"]\nreference = "macos-nfs"\n'
        macos = all_passed("macos-nfs")
        macos["scenarios"][0]["outcome"] = "skipped"
        nfs = all_passed("linux-nfs")
        nfs["scenarios"][0]["outcome"] = "skipped"
        # macos-nfs and linux-nfs agree with each other; macos-nfs still
        # differs from the default reference, so the run fails on that only.
        with Run([all_passed("linux-fuse"), macos, nfs], toml=toml) as r:
            self.assertEqual(r.code, 1)
        self.assertIn("| mismatch | `macos-nfs` |", r.stdout)
        self.assertNotIn("| mismatch | `linux-nfs` |", r.stdout)

    def test_bad_inputs_are_exit_2(self):
        with tempfile.TemporaryDirectory() as d:
            bad = os.path.join(d, "bad.json")
            with open(bad, "w") as fh:
                fh.write("{not json")
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(parity.main([bad]), 2)
                self.assertEqual(parity.main([os.path.join(d, "nope.json")]), 2)
        with Run([dict(all_passed("linux-fuse"), schema=2)]) as r:
            self.assertEqual(r.code, 2)
        with Run([results("linux-fuse", {"a": "exploded"})]) as r:
            self.assertEqual(r.code, 2)

    def test_additive_result_fields_are_accepted(self):
        docs = [
            all_passed("linux-fuse", s3_backend="docker", frontend="fuse"),
            all_passed("linux-fuse-process", s3_backend="process", frontend="fuse"),
        ]
        with Run(docs) as r:
            self.assertEqual(r.code, 0, r.stdout)


if __name__ == "__main__":
    unittest.main()
