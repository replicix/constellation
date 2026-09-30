#!/usr/bin/env bash
# Build hospike (needs gcc and liburing headers: dnf install liburing-devel /
# apt install liburing-dev).
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$HERE/build"
cc -O2 -g -Wall -Wextra -Wno-unused-parameter -Wno-missing-field-initializers -Wno-format-truncation \
	-o "$HERE/build/hospike" "$HERE/src/hospike.c" -luring -lpthread
echo "built $HERE/build/hospike"
