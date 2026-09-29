#!/usr/bin/env bash
# Install the native S3 test servers the harness's `--s3-backend process`
# needs: versitygw (the S3 server) and toxiproxy-server (fault injection).
#
#   tests/ci/install-native-s3.sh [DEST_DIR]      # default: $HOME/.local/bin
#
# then put DEST_DIR on PATH, or point CONSTELLATION_VERSITYGW_BIN /
# CONSTELLATION_TOXIPROXY_BIN at the binaries (the harness also looks in
# $HOME/.local/bin on its own).
#
# Modes (NATIVE_S3_INSTALL, default auto):
#   release  GitHub release archives, verified against the sha256 pinned below.
#   go       `go install` at the pinned versions. Integrity comes from the Go
#            module proxy + checksum database (go.sum / sum.golang.org).
#   auto     release when every sum for this platform is filled in below,
#            else go (needs a Go toolchain; preinstalled on GitHub runners).
#
# Pins. Bump the versions here and refill the sums together.
# versitygw must enforce PutObject `If-None-Match: *` (constellation refuses a
# backend that does not); its posix backend does from v1.0.18 on (v1.0.14, the
# version of the versity/versitygw image used elsewhere, does not).
VERSITYGW_VERSION="${VERSITYGW_VERSION:-v1.8.0}"
TOXIPROXY_VERSION="${TOXIPROXY_VERSION:-v2.12.0}"   # = the ghcr.io/shopify/toxiproxy:2.12.0 image
#
# TODO(CI owner): the sha256 sums below are intentionally EMPTY. They could
# not be verified when this script was written (release downloads were not
# reachable from the authoring sandbox) and must not be guessed. Fill them from
# the release pages (or `sha256sum` a downloaded asset) per platform; until a
# platform's two sums are set, `auto` uses the `go` mode for it. The asset
# names below follow the projects' usual goreleaser / release naming and are
# likewise unverified: fix them together with the sums.
#
#   versitygw_<version>_<Linux|Darwin>_<x86_64|arm64>.tar.gz  (contains `versitygw`)
#   toxiproxy-server-<linux|darwin>-<amd64|arm64>             (a bare binary)
SHA_VERSITYGW_linux_amd64=""
SHA_VERSITYGW_linux_arm64=""
SHA_VERSITYGW_darwin_amd64=""
SHA_VERSITYGW_darwin_arm64=""
SHA_TOXIPROXY_linux_amd64=""
SHA_TOXIPROXY_linux_arm64=""
SHA_TOXIPROXY_darwin_amd64=""
SHA_TOXIPROXY_darwin_arm64=""

set -euo pipefail

DEST="${1:-${HOME}/.local/bin}"
MODE="${NATIVE_S3_INSTALL:-auto}"
mkdir -p "$DEST"
DEST="$(cd "$DEST" && pwd)"

case "$(uname -s)" in
    Linux) OS=linux; OS_TITLE=Linux ;;
    Darwin) OS=darwin; OS_TITLE=Darwin ;;
    *) echo "install-native-s3: unsupported OS $(uname -s)" >&2; exit 1 ;;
esac
case "$(uname -m)" in
    x86_64|amd64) ARCH=amd64; ARCH_TITLE=x86_64 ;;
    arm64|aarch64) ARCH=arm64; ARCH_TITLE=arm64 ;;
    *) echo "install-native-s3: unsupported arch $(uname -m)" >&2; exit 1 ;;
esac

sha_vgw_var="SHA_VERSITYGW_${OS}_${ARCH}"
sha_tox_var="SHA_TOXIPROXY_${OS}_${ARCH}"
SHA_VGW="${!sha_vgw_var}"
SHA_TOX="${!sha_tox_var}"

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
    else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

fetch_verified() { # url sha256 out
    curl -fsSL --retry 3 -o "$3" "$1"
    local got
    got="$(sha256_of "$3")"
    if [ "$got" != "$2" ]; then
        echo "install-native-s3: sha256 mismatch for $1" >&2
        echo "  expected $2" >&2
        echo "  got      $got" >&2
        exit 1
    fi
}

install_release() {
    local tmp
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' RETURN
    local v="${VERSITYGW_VERSION}"
    fetch_verified \
        "https://github.com/versity/versitygw/releases/download/${v}/versitygw_${v}_${OS_TITLE}_${ARCH_TITLE}.tar.gz" \
        "$SHA_VGW" "$tmp/versitygw.tar.gz"
    tar -xzf "$tmp/versitygw.tar.gz" -C "$tmp" versitygw
    install -m 0755 "$tmp/versitygw" "$DEST/versitygw"
    fetch_verified \
        "https://github.com/Shopify/toxiproxy/releases/download/${TOXIPROXY_VERSION}/toxiproxy-server-${OS}-${ARCH}" \
        "$SHA_TOX" "$tmp/toxiproxy-server"
    install -m 0755 "$tmp/toxiproxy-server" "$DEST/toxiproxy-server"
}

install_go() {
    command -v go >/dev/null 2>&1 || {
        echo "install-native-s3: need a Go toolchain for the go mode (or fill in the sha256 pins above)" >&2
        exit 1
    }
    local tmp
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' RETURN
    # A private GOBIN so the toxiproxy binary, which `go install` names
    # `server`, cannot clobber anything else in DEST.
    GOBIN="$tmp" go install "github.com/versity/versitygw/cmd/versitygw@${VERSITYGW_VERSION}"
    GOBIN="$tmp" go install "github.com/Shopify/toxiproxy/v2/cmd/server@${TOXIPROXY_VERSION}"
    install -m 0755 "$tmp/versitygw" "$DEST/versitygw"
    install -m 0755 "$tmp/server" "$DEST/toxiproxy-server"
}

case "$MODE" in
    release)
        if [ -z "$SHA_VGW" ] || [ -z "$SHA_TOX" ]; then
            echo "install-native-s3: no sha256 pinned for ${OS}/${ARCH}; fill in the SHA_* pins in this script" >&2
            exit 1
        fi
        install_release ;;
    go) install_go ;;
    auto)
        if [ -n "$SHA_VGW" ] && [ -n "$SHA_TOX" ]; then
            install_release
        else
            echo "install-native-s3: no sha256 pins for ${OS}/${ARCH}, falling back to go install"
            install_go
        fi ;;
    *) echo "install-native-s3: NATIVE_S3_INSTALL must be release|go|auto" >&2; exit 2 ;;
esac

echo "installed into $DEST:"
"$DEST/versitygw" --version | head -1 || true
"$DEST/toxiproxy-server" -version || true
case ":$PATH:" in
    *":$DEST:"*) ;;
    *) echo "note: $DEST is not on PATH; export PATH=\"$DEST:\$PATH\" or set CONSTELLATION_VERSITYGW_BIN / CONSTELLATION_TOXIPROXY_BIN" ;;
esac
