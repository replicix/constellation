#!/usr/bin/env bash
# Shared helpers for the test suites: filesystem creation, FUSE
# mount/unmount lifecycle, and cleanup. Source, then call fs_setup.
#
# Callers get: $BIN $WORK $BACKEND $MNT $STATE $MOUNT_LOG, fs_create,
# fs_mount [extra mount args...], fs_unmount, say.

BIN="${CONSTELLATION_BIN:-${CARGO_TARGET_DIR:-target}/debug/constellation}"
MOUNT_PID=""

say() { echo "== $*"; }

# fs_setup [backend-url] — prepare workdir; default backend is a local dir.
fs_setup() {
    WORK=$(mktemp -d /tmp/constellation-test.XXXXXX)
    BACKEND="${1:-$WORK/backend}"
    MNT="$WORK/mnt"
    STATE="$WORK/state"
    MOUNT_LOG="$WORK/mount.log"
    mkdir -p "$MNT"
    trap fs_cleanup EXIT
}

fs_create() {
    "$BIN" fs create --s3 "$BACKEND" --chunk-size 1048576 --compression zstd:3
}

# fs_mount [extra mount flags...]
fs_mount() {
    "$BIN" mount --s3 "$BACKEND" "$MNT" --state-dir "$STATE" "$@" \
        >>"$MOUNT_LOG" 2>&1 &
    MOUNT_PID=$!
    local i
    for i in $(seq 1 100); do
        mountpoint -q "$MNT" && return 0
        kill -0 "$MOUNT_PID" 2>/dev/null || { echo "FAIL: mount process died"; return 1; }
        sleep 0.1
    done
    echo "FAIL: mount did not appear"
    return 1
}

fs_unmount() {
    fusermount3 -u "$MNT" 2>/dev/null || fusermount -u "$MNT" 2>/dev/null || true
    if [ -n "$MOUNT_PID" ]; then
        wait "$MOUNT_PID" 2>/dev/null || true
        MOUNT_PID=""
    fi
}

fs_cleanup() {
    local rc=$?
    fs_unmount
    if [ $rc -ne 0 ] && [ -s "${MOUNT_LOG:-}" ]; then
        echo "--- mount log ---"
        cat "$MOUNT_LOG"
    fi
    rm -rf "$WORK"
}
