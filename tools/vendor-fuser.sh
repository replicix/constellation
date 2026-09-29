#!/usr/bin/env bash
# Re-vendor fuser and re-apply Constellation's patch set (plan 31 §6.11,
# vendor/fuser/CONSTELLATION-PATCH.md), modelled on Mountpoint's
# vendor-fuser.sh: fetch the pristine upstream crate, lay it into
# vendor/fuser, then `git apply` every vendor/fuser/patches/*.patch in
# order. Any hunk that no longer applies fails the script loudly, and the
# working copy is restored: a patch is never silently dropped.
#
#   tools/vendor-fuser.sh 0.18.0              # from crates.io
#   tools/vendor-fuser.sh --from <dir>        # an unpacked crate (offline)
#   tools/vendor-fuser.sh --check [<version>] # only verify the patches apply
#
# After a version change: update the version and checksum in
# CONSTELLATION-PATCH.md, `cargo update -p fuser`, run the frontend's tests
# and the handover scenarios.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
vendor="$root/vendor/fuser"
patches="$vendor/patches"
check=0
from=""
version=""

while [ $# -gt 0 ]; do
  case "$1" in
    --check) check=1 ;;
    --from) from="$2"; shift ;;
    -h|--help) sed -n '2,16p' "$0"; exit 0 ;;
    *) version="$1" ;;
  esac
  shift
done

if [ -z "$from" ] && [ -z "$version" ]; then
  version=$(sed -n 's/^version = "\(.*\)"/\1/p' "$vendor/Cargo.toml" | head -1)
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

if [ -z "$from" ]; then
  echo "fetching fuser $version from crates.io" >&2
  curl -fsSL "https://static.crates.io/crates/fuser/fuser-$version.crate" -o "$work/fuser.crate"
  tar -xzf "$work/fuser.crate" -C "$work"
  from="$work/fuser-$version"
fi
[ -f "$from/src/session.rs" ] || { echo "error: $from is not an unpacked fuser crate" >&2; exit 1; }

# The pristine tree, laid out as vendor/fuser is (library files only).
pristine="$work/pristine"
mkdir -p "$pristine"
cp -r "$from/src" "$from/build.rs" "$from/Cargo.toml" "$from/README.md" "$from/LICENSE.md" "$pristine/"

# Apply the patch set to a copy first: nothing in vendor/fuser changes
# unless every patch applies cleanly.
staged="$work/staged"
cp -r "$pristine" "$staged"
shopt -s nullglob
set -- "$patches"/*.patch
[ $# -gt 0 ] || { echo "error: no patches in $patches" >&2; exit 1; }
for patch in "$@"; do
  echo "applying $(basename "$patch")" >&2
  if ! (cd "$staged" && git apply --check --verbose -p1 "$patch" >&2 && git apply -p1 "$patch"); then
    echo "error: $(basename "$patch") does not apply to fuser ${version:-from $from}:" \
         "re-make its hunks by hand (see vendor/fuser/CONSTELLATION-PATCH.md)" >&2
    exit 1
  fi
done

if [ "$check" = 1 ]; then
  if diff -r -q -x patches -x CONSTELLATION-PATCH.md "$staged" "$vendor" >&2; then
    echo "ok: the patch set reproduces vendor/fuser exactly" >&2
  else
    echo "error: vendor/fuser differs from pristine + patches (regenerate the patch)" >&2
    exit 1
  fi
  exit 0
fi

for f in src build.rs Cargo.toml README.md LICENSE.md; do
  rm -rf "${vendor:?}/$f"
  cp -r "$staged/$f" "$vendor/$f"
done
echo "vendor/fuser now holds fuser ${version:-from $from} with $# patch(es) applied" >&2
