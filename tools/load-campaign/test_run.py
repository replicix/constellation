"""Run: python3 -m unittest tools/load-campaign/test_run.py"""
import fnmatch
import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("run", Path(__file__).with_name("run.py"))
run = importlib.util.module_from_spec(spec)
spec.loader.exec_module(run)


class BackupBudget(unittest.TestCase):
    def test_delaying_profiles(self):
        # F3 of the 2026-10-24 validation: net-wan (60 ms one way, 93-117 ms
        # measured) is far past the 5 ms backup budget.
        for p in ["net-wan", "net-lossy", "net:delay:60ms:20ms", "cpu25,net-wan", "net:delay:3ms"]:
            self.assertTrue(run.delays_past_backup_budget(p), p)

    def test_others(self):
        for p in ["baseline", "net-lan", "net-flap", "net:flap", "net:delay:1ms", "s3-jitter",
                  "pause:takeover+300:15000@b"]:
            self.assertFalse(run.delays_past_backup_budget(p), p)

    def test_scenarios(self):
        def needs(s):
            return any(fnmatch.fnmatchcase(s, p) for p in run.NEEDS_BACKUP)

        for s in ["backup-failover", "backup-failover-with-delegation", "delegate-root-loss",
                  "load-probe-takeover", "lock-failover"]:
            self.assertTrue(needs(s), s)
        for s in ["lock-holder-killed-contention", "load-probe", "delegate-root-loss-ttl",
                  "flock-cross-node", "ack-latency"]:
            self.assertFalse(needs(s), s)


if __name__ == "__main__":
    unittest.main()
