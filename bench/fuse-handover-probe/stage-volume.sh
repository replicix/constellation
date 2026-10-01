#!/usr/bin/env bash
# Plan 37 K0 Track A: the `NodeStageVolume` shape against the real daemon.
#
# Creates a filesystem on the local file backend, starts a `constellation`
# daemon on it, then runs `stage_volume_probe` (crates/cli/examples/):
# mount(2) here, hand the descriptor to the daemon over its control socket
# (`view.mount` with `MountSource::PreopenedFd`), read and write through
# the mountpoint, detach. Needs root, so `--in-container` runs the same
# thing in a privileged container.
set -euo pipefail
repo=$(cd "$(dirname "$0")/../.." && pwd)
w=${WORK:-/tmp/k0a-stage}

if [[ ${1:-} == --in-container ]]; then
  exec docker run --rm --privileged --device /dev/fuse \
    -v "$repo:$repo" -w "$repo" ubuntu:24.04 sh -c \
    "apt-get install -y -qq fuse3 util-linux >/dev/null 2>&1 ||
     { apt-get update -qq >/dev/null 2>&1 && apt-get install -y -qq fuse3 util-linux >/dev/null 2>&1; }
     exec $0"
fi

bin=$repo/target/debug/constellation
probe=$repo/target/debug/examples/stage_volume_probe
rm -rf "$w"; mkdir -p "$w"/{s3,state,mnt-a,mnt-b}
trap 'kill %1 2>/dev/null || true; umount "$w/mnt-b" 2>/dev/null || true' EXIT

"$bin" fs create k0a --s3 "file://$w/s3"
"$bin" mount / "$w/mnt-a" --s3 "file://$w/s3" --state-dir "$w/state" --foreground \
    >"$w/daemon.log" 2>&1 &
for _ in $(seq 150); do mountpoint -q "$w/mnt-a" && break; sleep 0.2; done
mountpoint -q "$w/mnt-a" || { echo "the daemon did not mount"; tail -30 "$w/daemon.log"; exit 1; }
echo "== daemon up, its own view at $w/mnt-a"

"$probe" --state-dir "$w/state" --mountpoint "$w/mnt-b"
echo "== daemon log (tail)"
tail -5 "$w/daemon.log"
