#!/usr/bin/env bash
# Run xfstests generic tests and compare failures with the known baseline.
set -euo pipefail

XFSTESTS_DIR="${XFSTESTS_DIR:-/opt/xfstests}"
BASELINE="${XFSTESTS_BASELINE:-/usr/local/lib/constellation/xfstests-baseline.txt}"
EXCLUDE="${XFSTESTS_EXCLUDE:-/usr/local/lib/constellation/xfstests-exclude.txt}"
prefix="xfstests-$(date +%s)-$$"
test_backend="s3://constellation-ci/$prefix-test"
scratch_backend="s3://constellation-ci/$prefix-scratch"
work=$(mktemp -d)
test_mnt="$work/test"
scratch_mnt="$work/scratch"
mkdir -p "$test_mnt" "$scratch_mnt" "$work/test-state" "$work/scratch-state"

cleanup() {
    fusermount3 -u "$test_mnt" 2>/dev/null || fusermount3 -uz "$test_mnt" 2>/dev/null || true
    fusermount3 -u "$scratch_mnt" 2>/dev/null || fusermount3 -uz "$scratch_mnt" 2>/dev/null || true
    [ -z "${test_pid:-}" ] || wait "$test_pid" 2>/dev/null || true
    [ -z "${scratch_pid:-}" ] || wait "$scratch_pid" 2>/dev/null || true
    rm -rf "$work"
}
trap cleanup EXIT

constellation fs create --s3 "$test_backend"
constellation fs create --s3 "$scratch_backend"
constellation mount --s3 "$test_backend" "$test_mnt" --state-dir "$work/test-state" \
    --allow-other --fs-name constellation-test >"$work/test-state/mount.log" 2>&1 &
test_pid=$!
for _ in $(seq 1 100); do
    mountpoint -q "$test_mnt" && break
    sleep 0.1
done
mountpoint -q "$test_mnt" || { echo "initial test mount did not appear"; exit 1; }
constellation mount --s3 "$scratch_backend" "$scratch_mnt" --state-dir "$work/scratch-state" \
    --allow-other --fs-name constellation-scratch >"$work/scratch-state/mount.log" 2>&1 &
scratch_pid=$!
for _ in $(seq 1 100); do
    mountpoint -q "$scratch_mnt" && break
    sleep 0.1
done
mountpoint -q "$scratch_mnt" || { echo "initial scratch mount did not appear"; exit 1; }
cat >"$XFSTESTS_DIR/local.config" <<EOF
export TEST_DEV=constellation-test
export TEST_DIR=$test_mnt
export SCRATCH_DEV=constellation-scratch
export SCRATCH_MNT=$scratch_mnt
export FSTYP=fuse
export FUSE_SUBTYP=.constellation
export TEST_FS_MOUNT_OPTS="-osource=$test_backend,state=$work/test-state"
export MOUNT_OPTIONS="-osource=$scratch_backend,state=$work/scratch-state"
EOF

cd "$XFSTESTS_DIR"
# xfstests normally reaches FUSE through mount(8). Constellation deliberately
# stays in the foreground, while mount(8) holds a helper lock until its child
# returns; that combination deadlocks. Route only the two FUSE mount sites
# directly through our daemonizing helper. The pinned ref makes these exact
# replacements a tripwire for upstream changes.
python3 - <<'PY'
from pathlib import Path

path = Path("common/rc")
text = path.read_text()
scratch = "\t_mount -t $FSTYP$FUSE_SUBTYP `_scratch_mount_options $*`"
scratch_direct = '\t[ "$FSTYP" = "fuse" ] || ' + scratch.lstrip()
test = "    _mount -t $FSTYP$FUSE_SUBTYP $TEST_OPTIONS $TEST_FS_MOUNT_OPTS $SELINUX_MOUNT_OPTIONS $* $TEST_DEV $TEST_DIR"
test_direct = '    [ "$FSTYP" = "fuse" ] || ' + test.lstrip()
if text.count(scratch) != 1 or text.count(test) != 1:
    raise SystemExit("pinned xfstests mount hooks changed")
text = text.replace(scratch, scratch_direct).replace(test, test_direct)
text = text.replace(
    '_scratch_unmount()\n{\n\tcase "$FSTYP" in',
    '_scratch_unmount()\n{\n\tcase "$FSTYP" in\n\tfuse)\n\t\t;;',
)
text = text.replace(
    '_test_unmount()\n{\n\tif [ "$FSTYP" == "overlay" ]; then',
    '_test_unmount()\n{\n\tif [ "$FSTYP" == "fuse" ]; then\n\t\ttrue\n'
    '\telif [ "$FSTYP" == "overlay" ]; then',
)
path.write_text(text)
PY
rm -rf results
mkdir -p results
# Tell check's first between-test consistency pass that TEST_DEV is already
# mounted. Without this marker it intentionally unmounts before the first test.
touch results/require_test
exclude_clean="$work/exclude"
awk '!/^#/ && NF {print $1}' "$EXCLUDE" >"$exclude_clean"
set +e
check_log="$work/constellation-check.log"
check_cmd=(./check)
[ "${XFSTESTS_DEBUG:-0}" != 1 ] || check_cmd=(bash -x ./check)
if [ -n "${XFSTESTS_TESTS:-}" ]; then
    selection=("$XFSTESTS_TESTS")
else
    selection=()
    for test_file in tests/generic/[0-9][0-9][0-9]; do
        [ ! -x "$test_file" ] || selection+=("${test_file#tests/}")
    done
fi
"${check_cmd[@]}" -E "$exclude_clean" "${selection[@]}" 2>&1 | tee "$check_log"
check_status=${PIPESTATUS[0]}
set -e

actual="$work/failures"
for bad in results/generic/*.out.bad; do
    [ -e "$bad" ] || continue
    id=${bad#results/}
    echo "${id%.out.bad}"
done | sort >"$actual"

expected="$work/baseline"
awk '!/^#/ && NF {print $1}' "$BASELINE" | sort >"$expected"
regressions=$(comm -23 "$actual" "$expected" || true)
improvements=$(comm -13 "$actual" "$expected" || true)
failed=$(wc -l <"$actual")
configured_excluded=$(awk '!/^#/ && NF {n++} END {print n+0}' "$EXCLUDE")
run=$(grep -Ec '^generic/[0-9]{3}' "$check_log" || true)
not_run=$(awk '/^Not run:/ {for (i=2; i<=NF; i++) if ($i ~ /^generic\/[0-9]{3}$/) n++} END {print n+0}' "$check_log")
passed=$((run - failed - not_run))
excluded=$((configured_excluded + not_run))
printf 'XFSTESTS TALLY: %d passed, %d failed, %d excluded (%d capability-skipped)\n' \
    "$passed" "$failed" "$excluded" "$not_run"

if [ -n "$improvements" ]; then
    echo "IMPROVEMENTS (remove from baseline):"
    echo "$improvements" | sed 's/^/  + /'
fi
if [ -n "$regressions" ]; then
    echo "REGRESSIONS:"
    echo "$regressions" | sed 's/^/  - /'
    exit 1
fi
# xfstests exits nonzero for baselined failures too; only setup failure matters
# after the two-way comparison above.
if [ "$check_status" -ne 0 ] && [ "$run" -eq 0 ]; then
    echo "xfstests failed before running any tests"
    sed 's/^/  /' "$check_log"
    [ ! -f results/check.log ] || sed 's/^/  /' results/check.log
    for log in "$work"/*-state/mount.log; do
        [ ! -f "$log" ] || { echo "mount log: $log"; sed 's/^/  /' "$log"; }
    done
    exit "$check_status"
fi
echo "XFSTESTS PASSED ($failed known failures)"
