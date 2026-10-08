#!/usr/bin/env bash
# Cross-target type-check census (`make check-cross`).
#
#   aarch64-apple-darwin     the whole default workspace, one `cargo check
#                            --keep-going`; each crate that fails to compile
#                            gets its own census row (crates that depend on a
#                            failing crate are not reached, so they only show
#                            up once it is fixed). On success one `workspace`
#                            row. fuser is checked with its `macos-no-mount`
#                            feature (its default wants macFUSE via
#                            pkg-config, absent when cross-checking).
#   x86_64-pc-windows-gnu    each library crate on its own (`-p <crate>`), so
#                            one failing crate does not hide the others.
#                            Library crates = every workspace member except
#                            crates/cli, crates/harness, crates/frontend-fuse
#                            and bench/*.
#
# C compilers/archivers for build scripts come from tools/zcc and tools/zar
# (zig cc / zig ar), so no cross toolchain or SDK is needed. Only
# `cargo check` runs: nothing is linked.
#
# Prints a census table (target, crate, ok/FAIL, first error line). Failures
# listed in tools/check-cross-known-failures.txt (`<target> <crate>`, `#`
# comments) are expected; the script exits nonzero only for
#   - a failure that is NOT listed (new regression), or
#   - a listed failure that now passes (STALE: remove it from the file).
# A darwin failure that names no crate (toolchain/setup problem) is `workspace`.
#
# Environment: CARGO (default cargo); CHECK_CROSS_TARGETS (space-separated
# subset of the two targets, default both; stale detection only covers the
# targets that ran).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
CARGO="${CARGO:-cargo}"
KNOWN="$ROOT/tools/check-cross-known-failures.txt"
TARGETS="${CHECK_CROSS_TARGETS:-aarch64-apple-darwin x86_64-pc-windows-gnu}"
LOGDIR="$(mktemp -d "${TMPDIR:-/tmp}/check-cross.XXXXXX")"

export CARGO_TERM_COLOR=never

# zig's spelling of each rust target.
zig_target() {
    case "$1" in
    aarch64-apple-darwin) echo aarch64-macos ;;
    x86_64-pc-windows-gnu) echo x86_64-windows-gnu ;;
    *) echo "check-cross: no zig target for $1" >&2; return 1 ;;
    esac
}

# Library crates: workspace members minus cli, harness, frontend-fuse, csi,
# chaos, pod-load and bench/*. constellation-frontend-fuse is the Linux
# (FreeBSD-ready) FUSE frontend, a platform frontend like cli's binary: FUSE
# does not exist on Windows (plan 35's frontend is WinFsp), so it is not a
# Windows library crate. It is still type-checked for macOS by the darwin
# workspace row. constellation-csi (plan 37) is a Kubernetes-only driver
# binary, not a default workspace member (so the darwin row's plain
# `cargo check`, which only reaches default-members, already skips it) and
# excluded here too. constellation-chaos (the multi-node consistency
# checker) and constellation-pod-load (plan 37 K5b's `upgrade-under-load`
# pod binary, also not a default workspace member) exercise POSIX-only
# semantics (permissions, symlinks, `pread`/`pwrite`) as their actual job,
# not incidentally: they are harness-adjacent test tooling, not product
# library crates, so they are excluded here the same way constellation-cli
# and constellation-harness are.
lib_crates() {
    sed -n '/^members *= *\[/,/^\]/p' Cargo.toml | grep -o '"[^"]*"' | tr -d '"' |
        while read -r dir; do
            case "$dir" in
            crates/cli | crates/harness | crates/frontend-fuse | crates/csi | crates/chaos | crates/pod-load | bench/*) continue ;;
            esac
            sed -n 's/^name *= *"\(.*\)"/\1/p' "$dir/Cargo.toml" | head -1
        done
}

declare -a ROWS=()       # "target|crate|status|detail"
declare -A RESULT=()     # "target crate" -> ok|FAIL

record() { # target crate status detail
    ROWS+=("$1|$2|$3|$4")
    RESULT["$1 $2"]="$3"
    printf '  %-24s %-34s %s\n' "$1" "$2" "$3" >&2
}

# First `error` line of each crate that failed to compile, from a cargo log:
# `<crate>|<line>` per crate (the errors of a crate precede its "could not
# compile" line).
failed_crates() { # logfile
    awk '
        /^error: could not compile `/ {
            c = $0; sub(/^error: could not compile `/, "", c); sub(/`.*/, "", c)
            print c "|" (first == "" ? $0 : first); first = ""; next
        }
        /^error: failed to run custom build command for `/ {
            c = $0; sub(/^error: failed to run custom build command for `/, "", c)
            sub(/ .*/, "", c); sub(/`.*/, "", c)
            print c "|" $0; next
        }
        /^error/ && first == "" { first = $0 }
    ' "$1"
}

run_check() { # target crate cargo-args...
    local target="$1" crate="$2"
    shift 2
    local up="${target//-/_}"
    up="${up^^}"
    local lo="${target//-/_}"
    local log="$LOGDIR/$target-$crate.log"
    local zt
    zt="$(zig_target "$target")"
    if env "CC_$lo=$ROOT/tools/zcc -target $zt" \
        "CXX_$lo=$ROOT/tools/zcc -target $zt" \
        "AR_$lo=$ROOT/tools/zar" \
        "CARGO_TARGET_${up}_LINKER=$ROOT/tools/zcc" \
        "$CARGO" check --target "$target" "$@" >"$log" 2>&1; then
        record "$target" "$crate" ok ""
        return
    fi
    if [ "$crate" = workspace ]; then
        local n=0 c first
        while IFS='|' read -r c first; do
            record "$target" "$c" FAIL "$first"
            n=$((n + 1))
        done < <(failed_crates "$log")
        [ "$n" -gt 0 ] && return
    fi
    local first
    first="$(grep -m1 -E '^error' "$log" | grep -v 'could not compile' || true)"
    [ -n "$first" ] || first="$(grep -m1 -E '^error' "$log" || echo 'see log')"
    record "$target" "$crate" FAIL "$first"
}

for target in $TARGETS; do
    case "$target" in
    aarch64-apple-darwin)
        run_check "$target" workspace --keep-going --features fuser/macos-no-mount
        ;;
    x86_64-pc-windows-gnu)
        for crate in $(lib_crates); do
            run_check "$target" "$crate" -p "$crate"
        done
        ;;
    *)
        echo "check-cross: unsupported target $target" >&2
        exit 2
        ;;
    esac
done

echo
echo "== cross-check census =="
printf '%-24s %-34s %-5s %s\n' TARGET CRATE RESULT "FIRST ERROR"
for row in "${ROWS[@]}"; do
    IFS='|' read -r t c s d <<<"$row"
    printf '%-24s %-34s %-5s %s\n' "$t" "$c" "$s" "$d"
done
echo "(logs: $LOGDIR)"

# Known failures: `<target> <crate>` per line, `#` comments.
declare -A KNOWN_SET=()
if [ -f "$KNOWN" ]; then
    while read -r t c rest; do
        case "$t" in '' | '#'*) continue ;; esac
        if [ -z "${c:-}" ]; then
            echo "check-cross: malformed line in $KNOWN: '$t'" >&2
            exit 2
        fi
        KNOWN_SET["$t $c"]=1
    done <"$KNOWN"
fi

status=0
echo
while IFS= read -r key; do
    if [ "${RESULT[$key]}" = FAIL ] && [ -z "${KNOWN_SET[$key]:-}" ]; then
        echo "NEW FAILURE: $key (not in tools/check-cross-known-failures.txt)"
        status=1
    fi
done < <(printf '%s\n' "${!RESULT[@]}" | sort)
while IFS= read -r key; do
    [ -n "$key" ] || continue
    t="${key%% *}"
    case " $TARGETS " in *" $t "*) ;; *) continue ;; esac
    r="${RESULT[$key]:-missing}"
    if [ "$r" = ok ]; then
        echo "STALE: remove from known failures: $key (now passes)"
        status=1
    elif [ "$r" = missing ]; then
        echo "STALE: remove from known failures: $key (now passes, or is no longer checked)"
        status=1
    fi
done < <(printf '%s\n' "${!KNOWN_SET[@]}" | sort)
if [ "$status" = 0 ]; then
    echo "check-cross: OK (only known failures, if any)"
fi
exit "$status"
