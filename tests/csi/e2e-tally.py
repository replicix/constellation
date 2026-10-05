#!/usr/bin/env python3
"""Tally e2e.test JUnit reports (tests/csi/e2e.sh): passed / failed /
skipped External Storage specs, every failed and skipped one with the
message the suite gave. Several reports (a chunked run) are merged; a spec
in more than one counts once, by its last report.

Usage: tests/csi/e2e-tally.py REPORT.xml...
"""
import sys
import xml.etree.ElementTree as ET

DRIVER = "External Storage [Driver: csi.constellation.dev] "


def first_line(text):
    text = (text or "").strip()
    return text.splitlines()[0][:300] if text else "(no message)"


specs = {}
suite_failures = []
for path in sys.argv[1:]:
    for case in ET.parse(path).getroot().iter("testcase"):
        name = case.get("name", "")
        failure = case.find("failure")
        if failure is None:
            failure = case.find("error")
        if DRIVER not in name:
            # BeforeSuite/AfterSuite and the like: not specs, but their
            # failure is the run's.
            if failure is not None:
                suite_failures.append((name, first_line(failure.get("message") or failure.text)))
            continue
        name = name.split(DRIVER, 1)[1]
        skipped = case.find("skipped")
        if skipped is not None and (skipped.get("message") or "").strip() == "skipped":
            specs.setdefault(name, ("not run", ""))
            continue
        if failure is not None:
            specs[name] = ("failed", first_line(failure.get("message") or failure.text))
        elif skipped is not None:
            why = first_line(skipped.get("message"))
            specs[name] = ("skipped", why.removeprefix("skipped - "))
        else:
            specs[name] = ("passed", "")

kinds = ("passed", "failed", "skipped", "not run")
by = {k: sorted((n, w) for n, (s, w) in specs.items() if s == k) for k in kinds}
failed = len(by["failed"]) + len(suite_failures)
print(
    f"passed {len(by['passed'])}  failed {failed}  skipped {len(by['skipped'])}"
    f"  not run {len(by['not run'])}  (of {len(specs)})"
)
for title, rows in (
    ("FAILED", suite_failures + by["failed"]),
    ("SKIPPED", by["skipped"]),
):
    if rows:
        print(f"\n{title}:")
        for name, why in rows:
            print(f"- {name}\n    {why}")
print("\nPASSED:")
for name, _ in by["passed"]:
    print(f"- {name}")
if by["not run"]:
    print("\nNOT RUN (outside the focus):")
    for name, _ in by["not run"]:
        print(f"- {name}")
