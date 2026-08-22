#!/usr/bin/env bash
# Stress suite: data-integrity stress (fio with verification) and
# metadata churn (stress-ng filesystem stressors) on a live mount.
#
#   tests/stress.sh [backend-url]
#
# Tunables: STRESS_SIZE (per-file fio size, default 32M),
#           STRESS_TIMEOUT (per stress-ng stressor, default 15s).
set -euo pipefail
. "$(dirname "$0")/lib.sh"

SIZE="${STRESS_SIZE:-32M}"
TIMEOUT="${STRESS_TIMEOUT:-15s}"

command -v fio >/dev/null || { echo "fio not installed"; exit 2; }
command -v stress-ng >/dev/null || { echo "stress-ng not installed"; exit 2; }

fs_setup "${1:-}"
say "fs create + mount"
fs_create
fs_mount

say "fio: sequential write + verify (sha1, $SIZE)"
fio --name=seq-verify --directory="$MNT" --size="$SIZE" \
    --rw=write --bs=1M --ioengine=psync --fallocate=none \
    --verify=sha1 --do_verify=1 --end_fsync=1 \
    --output-format=terse | awk -F';' '{print "  bw="$48" iops="$49}' || exit 1

say "fio: random write + verify (crc32c, 4k-256k blocks, $SIZE)"
fio --name=rand-verify --directory="$MNT" --size="$SIZE" \
    --rw=randwrite --bsrange=4k-256k --ioengine=psync --fallocate=none \
    --verify=crc32c --do_verify=1 --end_fsync=1 \
    --output-format=terse >/dev/null || exit 1

say "fio: two concurrent writers + verify ($SIZE each)"
fio --name=par-verify --directory="$MNT" --size="$SIZE" --numjobs=2 \
    --rw=randwrite --bs=64k --ioengine=psync --fallocate=none \
    --verify=crc32c --do_verify=1 --end_fsync=1 --group_reporting \
    --output-format=terse >/dev/null || exit 1

say "stress-ng: dentry/dir/rename/symlink/open churn ($TIMEOUT each)"
for stressor in dentry dir rename symlink open; do
    stress-ng --temp-path "$MNT" --"$stressor" 2 \
        --timeout "$TIMEOUT" --metrics-brief 2>&1 | grep -E "run time|$stressor" | sed 's/^/  /'
    rc=${PIPESTATUS[0]}
    [ "$rc" -eq 0 ] || { echo "FAIL: stress-ng $stressor rc=$rc"; exit 1; }
done

say "post-stress sanity: mount still healthy"
echo ok > "$MNT/post-stress-canary"
[ "$(cat "$MNT/post-stress-canary")" = "ok" ]
rm "$MNT/post-stress-canary"

fs_unmount
echo "STRESS TEST PASSED"
