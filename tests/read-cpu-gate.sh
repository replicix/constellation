#!/usr/bin/env bash
# Daemon CPU-seconds per GiB and peak RSS on a real Constellation mount,
# measured with fio (plan 38 §6). This is the read path's *cost* gate: the
# rate gate is `tests/perf-gate.sh`, which measures throughput; this one
# measures what the daemon spends to serve it, because plan 38's whole
# point (verify-once, io_uring, zero-copy, passthrough) is to serve the
# same bytes for less CPU and less RAM.
#
# Deliberately modelled on `bench/fuse-read-path`'s method — the same
# workload shapes, the same "sample the daemon around the fio window"
# discipline — but against the real daemon, not the bench's minimal
# libfuse filesystem, so the numbers are Constellation's own and are
# never comparable to the bench's (plan 38 §4, §6).
#
# It must run unprivileged: there is no `drop_caches` here. "Cold" is a
# fresh mount whose disk cache directory was emptied (rebuilt by the
# startup scan), which is also the only shape that exercises the fetch
# path; "warm disk" is a fresh mount over a full disk cache (empty
# memory tier, empty kernel page cache for the new FUSE inodes); "warm
# memory" is the same mount read again with O_DIRECT, so every read
# reaches the daemon and is served from the chunk memory cache.
#
# Usage:
#   tests/read-cpu-gate.sh                    # gate against the baseline
#   READ_CPU_BLESS=1 tests/read-cpu-gate.sh   # rewrite the baseline
#   READ_CPU_LANES="cold-seq-1m" tests/read-cpu-gate.sh
#   READ_CPU_LANES="$(tests/read-cpu-gate.sh --size-lanes)" tests/read-cpu-gate.sh
#
# The read-size lanes (plan 38 Z4b, opt-in, never in the default set and
# never gated: they have no baseline) are `rand-<S>-dio` and `seq-<S>-dio`
# for S in 64k 128k 256k 512k 1m: O_DIRECT reads of exactly S bytes, 8
# random readers for READ_CPU_RAND_SECONDS or one sequential pass, over
# `seq1_0` on the mount `cold-seq-1m` just filled. On `uring` that pass
# admitted every chunk to the memory tier, so these lanes are memory hits;
# on `uring_zc` it left them verified on disk and out of the tier (a
# zero-copy read admits nothing), so with
# CONSTELLATION_FUSE_ZERO_COPY_MIN_READ=0 every one of their reads is
# zero-copy. The pair is the zero-copy threshold's measurement: a zero-copy
# read of S bytes against the memory hit it replaces, per S.
#
# Knobs (all optional):
#   CONSTELLATION_BIN       binary under test (default target/release/constellation)
#   READ_CPU_BASELINE       baseline JSON (default tests/read-cpu-baseline.json)
#   READ_CPU_OUT            where the JSON lines go (default $WORK/results.jsonl);
#                           each carries the daemon's memcache hit/miss
#                           deltas over the window as well as its CPU/RSS
#   READ_CPU_LANES          space-separated subset of the lanes below
#   READ_CPU_REPEATS        repeats per lane, median reported (default 3)
#   READ_CPU_BLESS=1        write the measured numbers to the baseline and pass
#   READ_CPU_SEQ_MIB        sequential file size (default 512)
#   READ_CPU_RAND_MIB       random-read file size (default 256)
#   READ_CPU_SMALL_FILES    small files, 64 KiB each (default 4096, the
#                           count bench/fuse-read-path's `smallfiles` used)
#   READ_CPU_RAND_SECONDS   random-read runtime per repeat (default 15)
#   READ_CPU_CHUNK_MIB      filesystem chunk size (default 4, the shape
#                           bench/fuse-read-path measured)
#   CONSTELLATION_FUSE_TRANSPORT  the daemon's transport (`auto`, the
#                           default, which puts this gate's cluster-lock
#                           mount on the ring; `uring`, the same; `dev-fuse` —
#                           plan 38 §2.4, Z2c), passed through to it and recorded
#                           in every result line; see tests/transport-matrix.sh,
#                           whose READ_CPU_GATE=1 mode runs this gate per leg
#
# Linux only (it reads `/proc/<pid>/stat`, `/proc/<pid>/status` and
# `clear_refs`): another OS SKIPs, as a missing fio does. A missing fio
# SKIPs loudly and exits 0 (CONVENTIONS.md's rule for the fio/stress
# lanes); anything else that fails, fails.
#
# A lane subset is measured exactly as it is in a full run: a warm lane
# selected without the lane that populates its tier gets an unmeasured
# warm-up pass instead (see `warmup_seq`).
set -euo pipefail
cd "$(dirname "$0")/.."

BASELINE="${READ_CPU_BASELINE:-tests/read-cpu-baseline.json}"
BIN="${CONSTELLATION_BIN:-${CARGO_TARGET_DIR:-target}/release/constellation}"
REPEATS="${READ_CPU_REPEATS:-3}"
SEQ_MIB="${READ_CPU_SEQ_MIB:-512}"
RAND_MIB="${READ_CPU_RAND_MIB:-256}"
SMALL_FILES="${READ_CPU_SMALL_FILES:-4096}"
RAND_SECONDS="${READ_CPU_RAND_SECONDS:-15}"
TRANSPORT="${CONSTELLATION_FUSE_TRANSPORT:-auto}"
ALL_LANES="cold-seq-1m warm-disk-seq-1m warm-mem-seq-1m rand-4k-dio smallfiles"
SIZES="64k 128k 256k 512k 1m"
SIZE_LANES=""
for size in $SIZES; do SIZE_LANES="$SIZE_LANES rand-$size-dio seq-$size-dio"; done
SIZE_LANES="${SIZE_LANES# }"
if [ "${1:-}" = --size-lanes ]; then
    echo "$SIZE_LANES"
    exit 0
fi
LANES="${READ_CPU_LANES:-$ALL_LANES}"
# The gate wants a *comparable* memory tier, not the host's share of RAM.
export CONSTELLATION_CHUNK_MEMCACHE_BYTES="${CONSTELLATION_CHUNK_MEMCACHE_BYTES:-1073741824}"
CACHE_SIZE="${READ_CPU_CACHE_SIZE:-4G}"
# 4 MiB, as `bench/fuse-read-path` used: a chunk many kernel reads deep,
# so the per-chunk costs of the read path (the whole-file load and hash
# plan 38 §2.3 removes, and later the transport's own per-reply cost) are
# visible instead of being one read per chunk.
CHUNK_MIB="${READ_CPU_CHUNK_MIB:-4}"
CHUNK_SIZE=$((CHUNK_MIB * 1024 * 1024))
FS_NAME=read-cpu-gate

[ "$(uname -s)" = Linux ] || {
    echo "SKIP: the read-cpu gate samples /proc/<pid>/{stat,status,clear_refs}; Linux only" >&2
    exit 0
}
command -v fio >/dev/null 2>&1 || {
    echo "SKIP: fio is not installed (read-cpu gate needs it); install fio to run this lane" >&2
    exit 0
}
command -v jq >/dev/null 2>&1 || { echo "FAIL: jq is required" >&2; exit 2; }
[ -x "$BIN" ] || { echo "FAIL: no constellation binary at $BIN" >&2; exit 2; }

export CONSTELLATION_BIN="$BIN"
# shellcheck source=tests/lib.sh
. "$(dirname "$0")/lib.sh"

fs_setup
OUT="${READ_CPU_OUT:-$WORK/results.jsonl}"
: >"$OUT"
say "read-cpu gate: $BIN"
say "work $WORK, results $OUT, transport $TRANSPORT, repeats $REPEATS"

mount_gate() { fs_mount --cache-size "$CACHE_SIZE" --write-mode through; }

# --- daemon sampling -------------------------------------------------------
# utime+stime from /proc/<pid>/stat (fields 14/15, in clock ticks) around
# each fio window; VmHWM from /proc/<pid>/status. `clear_refs` code 5
# resets the peak, which a process may do to itself — best effort, so a
# kernel or a hardening profile without it degrades to a monotonic peak
# rather than failing the gate.
TICKS=$(getconf CLK_TCK)

daemon_cpu_ticks() {
    awk '{ n = split($0, f, ") "); split(f[n], g, " "); print g[12] + g[13] }' \
        "/proc/$MOUNT_PID/stat"
}

daemon_field_kib() { awk -v k="$1:" '$1 == k { print $2 }' "/proc/$MOUNT_PID/status"; }

reset_peak_rss() { echo 5 >"/proc/$MOUNT_PID/clear_refs" 2>/dev/null || true; }

# What the mount actually negotiated (plan 38 §5's per-mount `transport`),
# as against `$TRANSPORT`, which is only what it was *asked* for: `auto`
# falls back to `dev_fuse` on a kernel below 6.14, with
# `fuse.enable_uring=N`, in a build without the `io-uring` feature, or
# under a sandbox that denies `io_uring_setup(2)`. Recorded in every
# result line so a ring leg that silently never reached the ring is
# visible in the numbers rather than taken on trust.
negotiated_transport() {
    "$BIN" status "$FS_NAME" --state-dir "$STATE" 2>/dev/null | python3 -c '
import json, sys
try:
    m = json.load(sys.stdin)["mounts"]
except Exception:
    m = []
print((m[0].get("transport") or "unknown") if m else "unknown")
' || echo unknown
}

# The daemon's own chunk-memory-cache counters, which say what the CPU
# numbers cannot on a noisy host: `misses` is one whole-chunk disk read
# plus (under `always`, or for an unverified entry) one blake3 pass, so
# the verify-once change of plan 38 §2.3 shows up here exactly, with no
# scheduler noise in it. The fourth number is the mount's zero-copy reads
# (plan 38 Z4b: reads the kernel served with one `READ_FIXED` from a chunk
# file, only ever on `uring_zc`), so a zero-copy leg shows how much of the
# lane actually took that path. `0 0 0 0` if the daemon cannot be reached.
memcache_counters() {
    "$BIN" status "$FS_NAME" --state-dir "$STATE" 2>/dev/null | python3 -c '
import json, sys
try:
    s = json.load(sys.stdin)
    c = s["cache"]
except Exception:
    print("0 0 0 0"); raise SystemExit
zc = sum(m.get("zero_copy_reads", 0) for m in s.get("fuse", {}).get("mounts", []))
print(c.get("memory_hits", 0), c.get("memory_misses", 0), c.get("memory_chunks", 0), zc)
' || echo "0 0 0 0"
}

# --- fio -------------------------------------------------------------------
fio_common=(--ioengine=psync --group_reporting=1 --output-format=json --randrepeat=1)

# run_fio LANE REP -- <fio args...>: samples the daemon around the run and
# appends one JSON line to $OUT.
run_fio() {
    local lane="$1" rep="$2"; shift 3
    local hits0 misses0 hits1 misses1 chunks1 zc0 zc1
    local json="$WORK/$lane.$rep.fio.json"
    reset_peak_rss
    local t0 c0 rc
    read -r hits0 misses0 _ zc0 <<<"$(memcache_counters)"
    c0=$(daemon_cpu_ticks)
    t0=$(date +%s.%N)
    rc=0
    fio "${fio_common[@]}" --output="$json" "$@" >/dev/null 2>"$WORK/$lane.$rep.fio.err" || rc=$?
    local t1 c1
    c1=$(daemon_cpu_ticks)
    t1=$(date +%s.%N)
    read -r hits1 misses1 chunks1 zc1 <<<"$(memcache_counters)"
    if [ "$rc" -ne 0 ]; then
        echo "FAIL: fio failed in lane $lane (rep $rep):" >&2
        cat "$WORK/$lane.$rep.fio.err" >&2
        return 1
    fi
    jq -c --arg lane "$lane" --arg transport "$TRANSPORT" \
        --arg negotiated "$(negotiated_transport)" \
        --argjson rep "$rep" \
        --argjson cpu_s "$(awk -v a="$c0" -v b="$c1" -v t="$TICKS" 'BEGIN{printf "%.4f", (b-a)/t}')" \
        --argjson wall_s "$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.3f", b-a}')" \
        --argjson rss_mib "$(awk -v k="$(daemon_field_kib VmRSS)" 'BEGIN{printf "%.1f", k/1024}')" \
        --argjson rss_hwm_mib "$(awk -v k="$(daemon_field_kib VmHWM)" 'BEGIN{printf "%.1f", k/1024}')" \
        --argjson chunk_mib "$CHUNK_MIB" \
        --argjson memcache "$CONSTELLATION_CHUNK_MEMCACHE_BYTES" \
        --argjson mem_hits "$((hits1 - hits0))" \
        --argjson mem_misses "$((misses1 - misses0))" \
        --argjson mem_chunks "$chunks1" \
        --argjson zc_reads "$((zc1 - zc0))" \
        '
        .jobs[0].read as $r
        | ($r.io_bytes / 1073741824) as $gib
        | {lane: $lane, rep: $rep, transport: $transport, negotiated: $negotiated,
           read_gib: ($gib * 1000 | round / 1000),
           bw_mib_s: ($r.bw / 1024 * 10 | round / 10),
           iops: ($r.iops | . * 10 | round / 10),
           clat_p50_us: (($r.clat_ns.percentile["50.000000"] // 0) / 1000 | . * 10 | round / 10),
           clat_p99_us: (($r.clat_ns.percentile["99.000000"] // 0) / 1000 | . * 10 | round / 10),
           daemon_cpu_s: $cpu_s,
           cpu_s_per_gib: (if $gib > 0 then ($cpu_s / $gib * 1000 | round / 1000) else null end),
           rss_mib: $rss_mib, rss_hwm_mib: $rss_hwm_mib, wall_s: $wall_s,
           chunk_mib: $chunk_mib, memcache_bytes: $memcache,
           memcache_hits: $mem_hits, memcache_misses: $mem_misses,
           memcache_chunks: $mem_chunks, zero_copy_reads: $zc_reads}
        ' "$json" >>"$OUT"
    tail -1 "$OUT" | jq -r '"  \(.lane) r\(.rep) [\(.negotiated)]: \(.bw_mib_s) MiB/s  \(.cpu_s_per_gib) cpu-s/GiB  \(.rss_hwm_mib) MiB peak RSS  \(.memcache_misses) memcache misses  \(.zero_copy_reads) zero-copy reads"'
}

# --- data ------------------------------------------------------------------
prepare_data() {
    say "prepare: $SEQ_MIB MiB sequential + $RAND_MIB MiB random + $SMALL_FILES x 64 KiB"
    mount_gate
    fio "${fio_common[@]}" --output=/dev/null --name=prep-seq --rw=write --bs=1M \
        --size="${SEQ_MIB}M" --filename="$MNT/seq1_0" --end_fsync=1 >/dev/null
    fio "${fio_common[@]}" --output=/dev/null --name=prep-rand --rw=write --bs=1M \
        --size="${RAND_MIB}M" --filename="$MNT/rand_0" --end_fsync=1 >/dev/null
    mkdir -p "$MNT/small"
    fio "${fio_common[@]}" --output=/dev/null --name=prep-small --rw=write --bs=64k \
        --size="$((SMALL_FILES * 64))k" --nrfiles="$SMALL_FILES" --directory="$MNT/small" \
        --filename_format='f.$filenum' --end_fsync=1 >/dev/null
    fs_unmount
}

# An emptied cache directory is the unprivileged stand-in for a cold
# cache: the startup scan rebuilds the (now empty) accounting, so every
# read fetches from the backend. The bytes are safe to drop — a clean
# unmount has uploaded them (`--write-mode through`).
empty_disk_cache() { rm -rf "$STATE/cache"; }

# --- lanes -----------------------------------------------------------------
lane_seq_buffered() { # NAME REP
    run_fio "$1" "$2" -- --name="$1" --rw=read --bs=1M --direct=0 \
        --size="${SEQ_MIB}M" --filename="$MNT/seq1_0"
}

lane_seq_direct() { # NAME REP
    run_fio "$1" "$2" -- --name="$1" --rw=read --bs=1M --direct=1 \
        --size="${SEQ_MIB}M" --filename="$MNT/seq1_0"
}

lane_rand_4k_dio() { # NAME REP
    run_fio "$1" "$2" -- --name="$1" --rw=randread --bs=4k --direct=1 --numjobs=8 \
        --time_based=1 --runtime="$RAND_SECONDS" --size="${RAND_MIB}M" --filename="$MNT/rand_0"
}

lane_rand_dio() { # NAME REP SIZE
    run_fio "$1" "$2" -- --name="$1" --rw=randread --bs="$3" --direct=1 --numjobs=8 \
        --time_based=1 --runtime="$RAND_SECONDS" --size="${SEQ_MIB}M" --filename="$MNT/seq1_0"
}

lane_seq_dio() { # NAME REP SIZE
    run_fio "$1" "$2" -- --name="$1" --rw=read --bs="$3" --direct=1 \
        --size="${SEQ_MIB}M" --filename="$MNT/seq1_0"
}

wants_size_lane() {
    local size
    for size in $SIZES; do
        wants "rand-$size-dio" && return 0
        wants "seq-$size-dio" && return 0
    done
    return 1
}

lane_smallfiles() { # NAME REP
    run_fio "$1" "$2" -- --name="$1" --rw=read --bs=64k --direct=0 \
        --size="$((SMALL_FILES * 64))k" --nrfiles="$SMALL_FILES" --directory="$MNT/small" \
        --filename_format='f.$filenum' --file_service_type=sequential --openfiles=1
}

wants() { case " $LANES " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

# An unmeasured sequential pass over `seq1_0`. A lane subset must measure
# the same thing it measures in a full run: the warm lanes are warm only
# because the lanes before them in the full order populated the tier they
# read from (`cold-seq-1m` fills the disk cache, `warm-disk-seq-1m` fills
# the memory tier). Selected out, that population has to happen here, or
# `READ_CPU_LANES="warm-disk-seq-1m"` silently measures a *cold* read —
# and `READ_CPU_BLESS=1` would then write it as the warm baseline.
warmup_seq() {
    fio "${fio_common[@]}" --output=/dev/null --name=warmup-seq --rw=read --bs=1M \
        --direct=0 --size="${SEQ_MIB}M" --filename="$MNT/seq1_0" >/dev/null
}

wants_warm_seq() { wants warm-disk-seq-1m || wants warm-mem-seq-1m; }

# `tests/lib.sh`'s `fs_create` hardcodes 1 MiB chunks; this lane picks its
# own, so it creates the filesystem itself.
say "creating the filesystem ($CHUNK_SIZE-byte chunks)"
"$BIN" fs create "$FS_NAME" --s3 "$BACKEND" --chunk-size "$CHUNK_SIZE" \
    --compression zstd:3 >/dev/null
prepare_data

for rep in $(seq 1 "$REPEATS"); do
    say "repeat $rep/$REPEATS"
    # Cold: the fetch path. Its own mount, because emptying the cache
    # needs the daemon gone.
    if wants cold-seq-1m || wants warm-disk-seq-1m || wants warm-mem-seq-1m || wants rand-4k-dio \
        || wants_size_lane; then
        empty_disk_cache
        mount_gate
        if wants cold-seq-1m; then
            lane_seq_buffered cold-seq-1m "$rep"
        elif wants_warm_seq || wants_size_lane; then
            warmup_seq # fetch seq1_0 into the disk cache this mount drops
        fi
        # The read-size lanes, on what the pass above left (header).
        for size in $SIZES; do
            if wants "rand-$size-dio"; then lane_rand_dio "rand-$size-dio" "$rep" "$size"; fi
            if wants "seq-$size-dio"; then lane_seq_dio "seq-$size-dio" "$rep" "$size"; fi
        done
        fs_unmount
        # Warm disk, cold memory tier, cold kernel page cache (a fresh
        # FUSE connection has no cached pages for its inodes).
        mount_gate
        if wants warm-disk-seq-1m; then
            lane_seq_buffered warm-disk-seq-1m "$rep"
        elif wants warm-mem-seq-1m; then
            warmup_seq # admit seq1_0's chunks to the memory tier
        fi
        # Warm memory tier: the chunks the lane above admitted, read
        # again with O_DIRECT so every read reaches the daemon.
        wants warm-mem-seq-1m && lane_seq_direct warm-mem-seq-1m "$rep"
        if wants rand-4k-dio; then
            # One pass to admit rand_0's chunks, then the measured run.
            fio "${fio_common[@]}" --output=/dev/null --name=warm-rand --rw=read --bs=1M \
                --direct=1 --size="${RAND_MIB}M" --filename="$MNT/rand_0" >/dev/null
            lane_rand_4k_dio rand-4k-dio "$rep"
        fi
        fs_unmount
    fi
    if wants smallfiles; then
        # Cold small files: 64 KiB reads of one short chunk per file, the
        # per-open cost the bench's `smallfiles` isolates.
        empty_disk_cache
        mount_gate
        lane_smallfiles smallfiles "$rep"
        fs_unmount
    fi
done

say "comparing with $BASELINE"
python3 - "$BASELINE" "$OUT" "$LANES" <<'PY'
import json, os, sys
from pathlib import Path
from statistics import median

baseline_path, out_path, lanes = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3].split()
rows = [json.loads(l) for l in out_path.read_text().splitlines() if l.strip()]
measured = {}
for lane in lanes:
    reps = [r for r in rows if r["lane"] == lane]
    if not reps:
        continue
    measured[lane] = {
        "cpu_s_per_gib": round(median(r["cpu_s_per_gib"] for r in reps), 3),
        "rss_hwm_mib": round(median(r["rss_hwm_mib"] for r in reps), 1),
        "bw_mib_s": round(median(r["bw_mib_s"] for r in reps), 1),
        # Informational, never gated: one miss is one whole-chunk disk
        # read (`load_shared`). It moves with prefetch timing, so it is
        # recorded for the next milestone to read, not compared.
        "memcache_misses": int(median(r.get("memcache_misses", 0) for r in reps)),
    }

bless = os.environ.get("READ_CPU_BLESS") == "1"
if baseline_path.exists():
    baseline = json.loads(baseline_path.read_text())
else:
    baseline, bless = {"lanes": {}}, True
    print(f"no baseline at {baseline_path}: creating it from this run")

# Deliberately loose: this gate catches a *large* regression (a lost
# memcache, a reintroduced re-hash, a leaked ring buffer), not the
# run-to-run and host-to-host spread. That spread is not small: the
# committed baseline was taken on an otherwise idle 32-CPU host, and the
# same tree on the same host under another build's load measured 2.9×
# the baseline's CPU per GiB on `smallfiles` and 1.5× on
# `warm-disk-seq-1m`. Peak RSS over the same runs moved by under 5%.
# Hence 4× on CPU and 1.5× on peak RSS (the tolerances below are the
# *extra* multiple over the baseline, so 3.0 and 0.5) — RSS is the reliable half of
# this gate and CPU the noisy one, and the regressions it is for (every
# read hashing again, every fetch reloaded from disk, a ring buffer pool
# leaked per mount) are multiples, not margins.
# `bench/fuse-read-path/RESULTS.md` is the reference these floors were
# chosen against, not a target to hold.
cpu_tol = float(baseline.get("cpu_tolerance", 3.0))   # limit = base * (1 + tol)
rss_tol = float(baseline.get("rss_tolerance", 0.5))
cpu_slack = float(baseline.get("cpu_abs_slack", 0.05))   # cpu-s/GiB
rss_slack = float(baseline.get("rss_abs_slack_mib", 64.0))

print(f"{'lane':20} {'cpu-s/GiB':>10} {'base':>8} {'limit':>8} "
      f"{'RSS MiB':>9} {'base':>8} {'limit':>8} {'MiB/s':>9} {'mc-miss':>8}")
failed = []
for lane, m in measured.items():
    base = baseline.get("lanes", {}).get(lane)
    if base is None:
        print(f"{lane:20} {m['cpu_s_per_gib']:10.3f} {'-':>8} {'-':>8} "
              f"{m['rss_hwm_mib']:9.1f} {'-':>8} {'-':>8} {m['bw_mib_s']:9.1f} "
              f"{m['memcache_misses']:8d}")
        continue
    cpu_limit = base["cpu_s_per_gib"] * (1 + cpu_tol) + cpu_slack
    rss_limit = base["rss_hwm_mib"] * (1 + rss_tol) + rss_slack
    print(f"{lane:20} {m['cpu_s_per_gib']:10.3f} {base['cpu_s_per_gib']:8.3f} {cpu_limit:8.3f} "
          f"{m['rss_hwm_mib']:9.1f} {base['rss_hwm_mib']:8.1f} {rss_limit:8.1f} {m['bw_mib_s']:9.1f} "
          f"{m['memcache_misses']:8d}")
    if m["cpu_s_per_gib"] > cpu_limit:
        failed.append(f"{lane} CPU/GiB {m['cpu_s_per_gib']:.3f} > {cpu_limit:.3f}")
    if m["rss_hwm_mib"] > rss_limit:
        failed.append(f"{lane} peak RSS {m['rss_hwm_mib']:.1f} MiB > {rss_limit:.1f}")

if bless:
    baseline.setdefault("lanes", {}).update(measured)
    baseline.setdefault("cpu_tolerance", cpu_tol)
    baseline.setdefault("rss_tolerance", rss_tol)
    baseline.setdefault("cpu_abs_slack", cpu_slack)
    baseline.setdefault("rss_abs_slack_mib", rss_slack)
    baseline["host"] = {
        "kernel": os.uname().release,
        "nproc": os.cpu_count(),
        "note": "host-specific; re-bless with READ_CPU_BLESS=1 on a different machine",
    }
    # The shape these numbers were taken at. Change any of it and the
    # baseline is not comparable — re-bless rather than reinterpret.
    shape = rows[0] if rows else {}
    baseline["config"] = {
        "chunk_mib": shape.get("chunk_mib"),
        "memcache_bytes": shape.get("memcache_bytes"),
        "seq_mib": int(os.environ.get("READ_CPU_SEQ_MIB", 512)),
        "rand_mib": int(os.environ.get("READ_CPU_RAND_MIB", 256)),
        "small_files": int(os.environ.get("READ_CPU_SMALL_FILES", 4096)),
        "rand_seconds": int(os.environ.get("READ_CPU_RAND_SECONDS", 15)),
        "repeats": int(os.environ.get("READ_CPU_REPEATS", 3)),
    }
    baseline_path.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n")
    print(f"BASELINE WRITTEN: {baseline_path}")
    raise SystemExit(0)

if failed:
    print("READ-CPU REGRESSION: " + "; ".join(failed), file=sys.stderr)
    raise SystemExit(1)
print("READ-CPU GATE PASSED")
PY
