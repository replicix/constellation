#!/usr/bin/env bash
# End-to-end smoke test: create FS, mount, exercise POSIX ops, unmount,
# remount, verify persistence.
#
# Usage:
#   tests/smoke.sh                    # local file backend (tmpdir)
#   tests/smoke.sh s3://bucket/prefix # any S3-compatible backend
#                                     # (credentials/endpoint via AWS_* env)
set -euo pipefail
. "$(dirname "$0")/lib.sh"
fs_setup "${1:-}"

say "fs create + doctor ($BACKEND)"
fs_create
"$BIN" doctor --s3 "$BACKEND"
"$BIN" fs create --s3 "$BACKEND" 2>/dev/null && { echo "FAIL: double create succeeded"; exit 1; }

say "mount"
fs_mount

say "basic namespace ops"
mkdir -p "$MNT/dir/sub"
echo "hello constellation" > "$MNT/dir/hello.txt"
[ "$(cat "$MNT/dir/hello.txt")" = "hello constellation" ]
ln -s hello.txt "$MNT/dir/link"
[ "$(readlink "$MNT/dir/link")" = "hello.txt" ]
[ "$(cat "$MNT/dir/link")" = "hello constellation" ]
mv "$MNT/dir/hello.txt" "$MNT/dir/sub/renamed.txt"
[ "$(cat "$MNT/dir/sub/renamed.txt")" = "hello constellation" ]
ls "$MNT/dir" | grep -qx "hello.txt" && { echo "FAIL: old name still present"; exit 1; }

say "multi-chunk file (3.5 MiB across 1 MiB chunks)"
dd if=/dev/urandom of="$WORK/random.bin" bs=1M count=3 2>/dev/null
dd if=/dev/urandom bs=512k count=1 2>/dev/null >> "$WORK/random.bin"
cp "$WORK/random.bin" "$MNT/dir/random.bin"
cmp "$WORK/random.bin" "$MNT/dir/random.bin"

say "partial in-place edit"
printf 'XYZ' | dd of="$MNT/dir/random.bin" bs=1 seek=2000000 conv=notrunc 2>/dev/null
printf 'XYZ' | dd of="$WORK/random.bin" bs=1 seek=2000000 conv=notrunc 2>/dev/null
cmp "$WORK/random.bin" "$MNT/dir/random.bin"

say "truncate"
truncate -s 1500000 "$MNT/dir/random.bin"
truncate -s 1500000 "$WORK/random.bin"
cmp "$WORK/random.bin" "$MNT/dir/random.bin"

say "append"
echo tail >> "$MNT/dir/random.bin"
echo tail >> "$WORK/random.bin"
cmp "$WORK/random.bin" "$MNT/dir/random.bin"

say "unlink while open"
echo "orphan data" > "$MNT/orphan.txt"
exec 7< "$MNT/orphan.txt"
rm "$MNT/orphan.txt"
[ ! -e "$MNT/orphan.txt" ]
[ "$(cat <&7)" = "orphan data" ]
exec 7<&-

say "rm/rmdir"
rm "$MNT/dir/link" "$MNT/dir/sub/renamed.txt"
rmdir "$MNT/dir/sub"
rmdir "$MNT/dir" 2>/dev/null && { echo "FAIL: rmdir non-empty succeeded"; exit 1; }

say "unmount"
fs_unmount

say "remount and verify persistence"
fs_mount
cmp "$WORK/random.bin" "$MNT/dir/random.bin"
[ -d "$MNT/dir" ]
[ ! -e "$MNT/orphan.txt" ]

say "cold cache read (fresh chunk cache, data pulled from backend)"
fs_unmount
rm -rf "$STATE/cache"
fs_mount
cmp "$WORK/random.bin" "$MNT/dir/random.bin"

say "status"
"$BIN" status --s3 "$BACKEND" | grep -q uuid

fs_unmount
echo "SMOKE TEST PASSED"
