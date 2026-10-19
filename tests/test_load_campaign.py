"""Unit tests for tools/load-campaign/run.py's summary.

    python3 -m unittest discover -s tests -p 'test_load_campaign.py' -v

An `inconclusive` cell is no pass: a campaign with more than the allowed share
of them fails, so a batch that proves too little does not stay green.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import tempfile
import unittest
from pathlib import Path

HERE = Path(os.path.dirname(os.path.abspath(__file__)))
spec = importlib.util.spec_from_file_location("run", HERE.parent / "tools/load-campaign/run.py")
run = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run)


def row(verdict):
    return {"phase": "p", "profile": "baseline", "scenario": "s", "seed": 1,
            "verdict": verdict, "seconds": 1, "host_lag": None, "dir": ""}


def summarize(verdicts, pct=10.0):
    with tempfile.TemporaryDirectory() as d, contextlib.redirect_stdout(io.StringIO()), \
            contextlib.redirect_stderr(io.StringIO()):
        return run.summarize(Path(d), [row(v) for v in verdicts], pct)


class InconclusiveShare(unittest.TestCase):
    def test_a_few_inconclusive_cells_are_a_pass(self):
        self.assertEqual(summarize(["inconclusive"] + ["passed"] * 9), 0)

    def test_more_than_the_share_fails_the_campaign(self):
        self.assertEqual(summarize(["inconclusive"] * 2 + ["passed"] * 8), 1)
        self.assertEqual(summarize(["inconclusive"] * 2 + ["passed"] * 8, 25.0), 0)

    def test_skipped_and_excluded_cells_are_not_judged(self):
        self.assertEqual(summarize(["inconclusive"] + ["passed"] * 9 + ["skipped"] * 5 + ["excluded"] * 5), 0)
        self.assertEqual(summarize(["skipped"] * 3), 0)

    def test_a_failed_cell_still_fails(self):
        self.assertEqual(summarize(["failed"] + ["passed"] * 20), 1)


if __name__ == "__main__":
    unittest.main()
