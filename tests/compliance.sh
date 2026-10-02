#!/usr/bin/env bash
# POSIX compliance suite (pjdfstest) with baseline regression tracking.
#
#   tests/compliance.sh [backend-url]
#
# Runs every pjdfstest category against a constellation mount and diffs
# the failures against tests/pjdfstest-baseline.txt:
#   - a failure NOT in the baseline  -> regression      -> exit 1
#   - a baseline entry that now passes -> improvement   -> warning to
#     update the baseline (kept green so fixes don't block)
#
# Requires root (pjdfstest switches uids) — run via the compose `test`
# profile. PJDFSTEST_DIR must hold the built binary + tests tree.
set -euo pipefail
. "$(dirname "$0")/lib.sh"

PJD="${PJDFSTEST_DIR:-/opt/pjdfstest}"
BASELINE="${PJDFSTEST_BASELINE:-$(dirname "$0")/pjdfstest-baseline.txt}"

if [ "$(id -u)" -ne 0 ]; then
    echo "compliance suite must run as root (pjdfstest switches uids)"
    exit 2
fi
[ -x "$PJD/pjdfstest" ] || { echo "pjdfstest binary not found at $PJD"; exit 2; }

fs_setup "${1:-}"
say "fs create + mount (allow-other for pjdfstest uid switching)"
fs_create
fs_mount --allow-other

# Which transport this run is actually gating (plan 38 §6: "pjdfstest
# 8798/8798 on every transport"). `CONSTELLATION_FUSE_TRANSPORT` (default
# `auto`, which keeps this cluster-lock mount on `/dev/fuse`; `uring` in
# `compliance-uring`) is a *request*: the ladder falls back to `/dev/fuse`
# whenever the kernel is too old, `fuse.enable_uring` is off, or the
# sandbox denies `io_uring_setup(2)` — and a container's default seccomp
# profile denies it silently. Printing what was negotiated is what keeps the ring leg
# from passing while testing nothing. `sed`, not `jq`/`python3`: the suite
# image carries neither.
transport=$("$BIN" status tests --state-dir "$STATE" 2>/dev/null \
    | sed -n 's/.*"transport": *"\([a-z_]*\)".*/\1/p' | head -1)
say "FUSE transport: ${transport:-unknown} (asked for ${CONSTELLATION_FUSE_TRANSPORT:-auto})"

SCRATCH="$MNT/pjdfstest-scratch"
mkdir -p "$SCRATCH"
RESULTS="$WORK/results.txt"
DETAILS="$WORK/details.txt"
: > "$RESULTS"
: > "$DETAILS"

# PJDFSTEST_ONLY="rename chown" restricts categories (fast iteration).
filter="${PJDFSTEST_ONLY:-}"

say "running pjdfstest categories${filter:+ (only: $filter)}"
total_ok=0
total_fail=0
for tfile in $(cd "$PJD/tests" && find . -name '*.t' | sort); do
    tid="${tfile#./}"
    if [ -n "$filter" ]; then
        cat="${tid%%/*}"
        case " $filter " in
            *" $cat "*) ;;
            *) continue ;;
        esac
    fi
    # Each .t is a TAP-emitting shell script; run it from a clean cwd
    # inside the mount.
    dir="$SCRATCH/$(echo "$tid" | tr '/' '_')"
    mkdir -p "$dir"
    out=$( (cd "$dir" && sh "$PJD/tests/$tid" 2>&1) || true)
    rm -rf "$dir" 2>/dev/null || true
    while IFS= read -r line; do
        case "$line" in
            # TAP: "# TODO" marks an expected failure — not a regression.
            not\ ok\ *\#\ TODO*) total_ok=$((total_ok + 1)) ;;
            ok\ *) total_ok=$((total_ok + 1)) ;;
            not\ ok\ *)
                total_fail=$((total_fail + 1))
                num=$(echo "$line" | awk '{print $3}')
                echo "$tid:$num" >> "$RESULTS"
                echo "$tid:$num  ${line#not ok $num }" >> "$DETAILS"
                ;;
        esac
    done <<< "$out"
done
sort -o "$RESULTS" "$RESULTS"

say "results: $total_ok passed, $total_fail failed"
# Plan 38 Z3b: whether FUSE passthrough served any of it (a privileged run
# on a kernel that offers it does; `opens_total` counts the opens the
# kernel was handed a backing file for, and `unavailable_reason` says why
# not). The status is joined onto one line first, so this reads compact
# and pretty-printed JSON alike: the `passthrough` object holds no nested
# object, so it ends at its first `}`. `sed`, as above.
passthrough=$("$BIN" status tests --state-dir "$STATE" 2>/dev/null \
    | tr -d '\n' \
    | sed -n 's/.*"passthrough": *{\([^}]*\)}.*/\1/p' \
    | tr ',' '\n' \
    | sed -n 's/^ *"\(enabled\|opens_total\|unavailable_reason\|refused_opens\)": *\(.*[^ ]\) *$/\1=\2/p' \
    | tr '\n' ' ')
say "FUSE passthrough: ${passthrough:-unknown}"

# Diff against the baseline (which may be absent or read-only).
BASE_SORTED="$WORK/baseline-sorted.txt"
{ grep -hv '^#' "$BASELINE" 2>/dev/null || true; } | sort > "$BASE_SORTED"
regressions=$(comm -23 "$RESULTS" "$BASE_SORTED" || true)
improvements=$(comm -13 "$RESULTS" "$BASE_SORTED" || true)

if [ -n "$improvements" ]; then
    say "IMPROVEMENTS (now passing; remove from baseline):"
    echo "$improvements" | sed 's/^/  + /'
fi
if [ -n "$regressions" ]; then
    say "REGRESSIONS (failing but not in baseline):"
    echo "$regressions" | sed 's/^/  - /'
    say "first regression details:"
    if [ "${PJDFSTEST_DETAILS:-}" = "all" ]; then
        sed 's/^/  ! /' "$DETAILS"
    else
        while IFS= read -r reg; do
            grep -F "$reg " "$DETAILS" | head -1 | sed 's/^/  ! /'
        done <<< "$(echo "$regressions" | head -40)"
    fi
    fs_unmount
    echo "COMPLIANCE TEST FAILED ($(echo "$regressions" | wc -l) regressions)"
    exit 1
fi

fs_unmount
echo "COMPLIANCE TEST PASSED (baseline: $total_fail known failures)"
