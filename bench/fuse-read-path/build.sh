#!/usr/bin/env bash
# Build chunkfs (inside a libfuse checkout), mkdata, and, when the system lacks
# them, liburing / libaio / fio from source. Everything lands in third_party/
# and build/.
#
#   ./build.sh            libfuse master + chunkfs, mkdata, deps as needed
#   ./build.sh --zc       also joannekoong/libfuse zero_copy_v7 + chunkfs-zc
#
# Env: LIBFUSE_REF (default master), LIBFUSE_ZC_REF (default zero_copy_v7),
#      FROM_SOURCE=1 builds liburing/fio from source even if the system has them.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
TP=$HERE/third_party
OUT=$HERE/build
LIBFUSE_REF=${LIBFUSE_REF:-master}
LIBFUSE_ZC_REF=${LIBFUSE_ZC_REF:-zero_copy_v7}
FROM_SOURCE=${FROM_SOURCE:-0}
WANT_ZC=0
[[ ${1:-} == --zc ]] && WANT_ZC=1
JOBS=$(nproc)
export PATH=$HOME/.local/bin:$PATH
mkdir -p "$TP" "$OUT"

log() { echo "== $*" >&2; }

# A symlink would break the binary's $ORIGIN runpath, so exec through a wrapper.
wrapper() { # target link
	rm -f "$2"
	printf '#!/bin/sh\nexec %q "$@"\n' "$1" >"$2"
	chmod +x "$2"
}

clone() { # dir url ref
	local dir=$1 url=$2 ref=$3
	if [[ ! -d $dir/.git ]]; then
		git clone -q "$url" "$dir"
	fi
	git -C "$dir" fetch -q origin "$ref" 2>/dev/null || true
	git -C "$dir" checkout -q "$ref" 2>/dev/null || git -C "$dir" checkout -q "origin/$ref"
}

command -v meson >/dev/null && command -v ninja >/dev/null || {
	log "meson/ninja missing: pip install --user meson ninja"
	exit 1
}

PKGCFG=""
if [[ $FROM_SOURCE == 1 ]] || ! pkg-config --exists liburing; then
	if [[ ! -f $TP/liburing/install/lib/pkgconfig/liburing.pc ]]; then
		log "building liburing from source"
		clone "$TP/liburing" https://github.com/axboe/liburing.git master
		(cd "$TP/liburing" && ./configure --prefix="$TP/liburing/install" >/dev/null &&
			make -j"$JOBS" -C src >/dev/null && make install >/dev/null)
	fi
	PKGCFG=$TP/liburing/install/lib/pkgconfig
fi
export PKG_CONFIG_PATH=${PKGCFG}${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}

# Add chunkfs to a libfuse tree's example/meson.build (idempotent).
wire_chunkfs() { # libfuse-dir extra-c-args
	local lf=$1 cargs=$2
	ln -sf "$HERE/src/chunkfs.c" "$lf/example/chunkfs.c"
	sed -i '/# chunkfs-begin/,/# chunkfs-end/d' "$lf/example/meson.build"
	cat >>"$lf/example/meson.build" <<EOF
# chunkfs-begin (bench/fuse-read-path)
executable('chunkfs', 'chunkfs.c',
           dependencies: [ thread_dep, libfuse_dep, cc.find_library('dl', required: false) ],
           c_args: [ $cargs ],
           install: false)
# chunkfs-end
EOF
}

build_libfuse() { # dir
	local lf=$1
	if [[ ! -f $lf/build/build.ninja ]]; then
		(cd "$lf" && meson setup build -Dexamples=true -Dtests=false -Dutils=false \
			-Denable-io-uring=true --buildtype=release >/dev/null)
	fi
	ninja -C "$lf/build" example/chunkfs >/dev/null
}

log "libfuse ($LIBFUSE_REF)"
clone "$TP/libfuse" https://github.com/libfuse/libfuse.git "$LIBFUSE_REF"
wire_chunkfs "$TP/libfuse" "'-Wall', '-Wextra', '-Wno-unused-parameter'"
build_libfuse "$TP/libfuse"
if ! grep -rq "define HAVE_URING" "$TP/libfuse/build/"; then
	log "WARNING: libfuse built without io-uring support (liburing too old or missing); uring modes will not work"
fi
wrapper "$TP/libfuse/build/example/chunkfs" "$OUT/chunkfs"
git -C "$TP/libfuse" rev-parse HEAD >"$OUT/libfuse.commit"

if [[ $WANT_ZC == 1 ]]; then
	log "libfuse zero-copy draft ($LIBFUSE_ZC_REF)"
	clone "$TP/libfuse-zc" https://github.com/joannekoong/libfuse.git "$LIBFUSE_ZC_REF"
	for p in "$HERE"/patches/libfuse-zc-*.patch; do
		[[ -e $p ]] || continue
		git -C "$TP/libfuse-zc" apply --check "$p" 2>/dev/null && git -C "$TP/libfuse-zc" apply "$p"
	done
	wire_chunkfs "$TP/libfuse-zc" "'-DCHUNKFS_ZC', '-Wall', '-Wno-unused-parameter'"
	build_libfuse "$TP/libfuse-zc"
	wrapper "$TP/libfuse-zc/build/example/chunkfs" "$OUT/chunkfs-zc"
	git -C "$TP/libfuse-zc" rev-parse HEAD >"$OUT/libfuse-zc.commit"
fi

log "mkdata"
cc -O2 -Wall -Wextra -o "$OUT/mkdata" "$HERE/src/mkdata.c"

fio_ok() { "$1" --enghelp 2>/dev/null | grep -qw libaio; }
if [[ $FROM_SOURCE == 0 ]] && command -v fio >/dev/null && fio_ok "$(command -v fio)"; then
	ln -sf "$(command -v fio)" "$OUT/fio"
else
	if [[ ! -x $TP/fio/fio ]] || ! fio_ok "$TP/fio/fio"; then
		AIO=""
		if [[ ! -e /usr/include/libaio.h ]]; then
			log "building libaio from source"
			clone "$TP/libaio" https://pagure.io/libaio.git master
			make -C "$TP/libaio" -j"$JOBS" >/dev/null 2>&1
			make -C "$TP/libaio" install prefix="$TP/libaio/install" >/dev/null 2>&1
			AIO=$TP/libaio/install
		fi
		log "building fio from source"
		clone "$TP/fio" https://github.com/axboe/fio.git master
		(cd "$TP/fio" && make clean >/dev/null 2>&1 || true
		 if [[ -n $AIO ]]; then
			LDFLAGS="-L$AIO/lib -Wl,-rpath,$AIO/lib" ./configure --disable-native --extra-cflags="-I$AIO/include" >/dev/null
		 else
			./configure --disable-native >/dev/null
		 fi
		 make -j"$JOBS" >/dev/null)
		fio_ok "$TP/fio/fio" || { log "fio built without libaio"; exit 1; }
	fi
	ln -sf "$TP/fio/fio" "$OUT/fio"
fi

log "done: $(ls "$OUT" | tr '\n' ' ')"
"$OUT/fio" --version
