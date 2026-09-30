#!/usr/bin/env bash
# FUSE read-path benchmark runner. See README.md.
#
#   ./run.sh [options] prepare                  generate the store
#   ./run.sh [options] cell MODE WORKLOAD CACHE one measured cell (CACHE: warm|cold)
#   ./run.sh [options] matrix                   modes x workloads x caches x repeats
#   ./run.sh [options] smoke                    every mode: mount, verify, short fio run
#
# Options (also settable as env vars in capitals, e.g. REPEATS=5):
#   --quick              small data set (data/store-quick), short runtimes
#   --store DIR          store directory (default data/store or data/store-quick)
#   --mnt DIR            mount point (default data/mnt)
#   --results FILE       JSONL output (default results/<host>-<utc-stamp>.jsonl)
#   --repeats N          repeats per cell in matrix (default 3)
#   --modes LIST         comma list (default: all modes this host can run)
#   --workloads LIST     comma list (default: all)
#   --caches LIST        comma list of warm,cold (default warm,cold; cold needs root)
#   --runtime S          seconds for the time-based workloads (default 30)
#   --chunk-size N       chunk size for prepare (default 4M)
#   --chunkfs-opts STR   extra chunkfs options for every mount (e.g. "--threads 16")
#   --read-ahead-kb N    set the FUSE bdi read_ahead_kb after mounting (root)
#   --keep-going         record verification failures and continue instead of aborting
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
BUILD=$HERE/build
QUICK=${QUICK:-0}
STORE=${STORE:-}
MNT=${MNT:-$HERE/data/mnt}
RESULTS=${RESULTS:-}
REPEATS=${REPEATS:-3}
MODES=${MODES:-}
WORKLOADS=${WORKLOADS:-}
CACHES=${CACHES:-}
RUNTIME=${RUNTIME:-}
CHUNK_SIZE=${CHUNK_SIZE:-4M}
CHUNKFS_OPTS=${CHUNKFS_OPTS:-}
READ_AHEAD_KB=${READ_AHEAD_KB:-}
KEEP_GOING=${KEEP_GOING:-0}

ALL_MODES=(copy memcache mmap splice splice-nomove vmsplice vmsplice-gift uring uring-bufpool uring-zc passthrough)
ALL_WORKLOADS=(seq1 seq1-dio seq8 rand4k-1j rand4k-8j rand4k-1j-dio rand4k-8j-dio rand128k-aio smallfiles)

die() { echo "run.sh: $*" >&2; exit 1; }
log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

ARGS=()
while (($#)); do
	case $1 in
	--quick) QUICK=1 ;;
	--store) STORE=$2; shift ;;
	--mnt) MNT=$2; shift ;;
	--results) RESULTS=$2; shift ;;
	--repeats) REPEATS=$2; shift ;;
	--modes) MODES=$2; shift ;;
	--workloads) WORKLOADS=$2; shift ;;
	--caches) CACHES=$2; shift ;;
	--runtime) RUNTIME=$2; shift ;;
	--chunk-size) CHUNK_SIZE=$2; shift ;;
	--chunkfs-opts) CHUNKFS_OPTS=$2; shift ;;
	--read-ahead-kb) READ_AHEAD_KB=$2; shift ;;
	--keep-going) KEEP_GOING=1 ;;
	-h|--help) sed -n '2,/^set -e/p' "$0" | sed 's/^# \{0,1\}//;/^set -e/d'; exit 0 ;;
	*) ARGS+=("$1") ;;
	esac
	shift
done
set -- "${ARGS[@]+"${ARGS[@]}"}"

IS_ROOT=0; [[ $(id -u) == 0 ]] && IS_ROOT=1
if [[ $QUICK == 1 ]]; then
	STORE=${STORE:-$HERE/data/store-quick}
	RUNTIME=${RUNTIME:-5}
	DATASET=(seq1:256M seq8:64M:8 rand:128M small:64K:256 odd:12595475)
	NSMALL=256
else
	STORE=${STORE:-$HERE/data/store}
	RUNTIME=${RUNTIME:-30}
	DATASET=(seq1:4G seq8:1G:8 rand:1G small:64K:4096 odd:12595475)
	NSMALL=4096
fi
HOST=$(hostname -s)
RESULTS=${RESULTS:-$HERE/results/$HOST-$(date -u +%Y%m%dT%H%M%SZ).jsonl}
RAW=${RESULTS%.jsonl}.raw

need_build() {
	[[ -x $BUILD/chunkfs && -x $BUILD/mkdata && -x $BUILD/fio ]] || die "run ./build.sh first"
}

# ---------- environment ----------

enable_uring() {
	local p=/sys/module/fuse/parameters/enable_uring
	[[ -e $p ]] || return 1
	[[ $(cat $p) == Y ]] && return 0
	[[ $IS_ROOT == 1 ]] || return 1
	echo Y >$p 2>/dev/null || return 1
	[[ $(cat $p) == Y ]]
}

mode_available() { # mode -> 0 if this host can run it, else prints why
	case $1 in
	uring|uring-bufpool)
		enable_uring || { echo "enable_uring is not Y (needs root to set)"; return 1; } ;;
	uring-zc)
		[[ -x $BUILD/chunkfs-zc ]] || { echo "build/chunkfs-zc missing (./build.sh --zc)"; return 1; }
		[[ $IS_ROOT == 1 ]] || { echo "needs root"; return 1; }
		enable_uring || { echo "enable_uring is not Y"; return 1; } ;;
	passthrough)
		[[ $IS_ROOT == 1 ]] || { echo "needs root (CAP_SYS_ADMIN)"; return 1; } ;;
	esac
	return 0
}

drop_caches() {
	[[ $IS_ROOT == 1 ]] || die "cold runs need root (drop_caches)"
	sync
	echo 3 >/proc/sys/vm/drop_caches
}

# ---------- data ----------

cmd_prepare() {
	need_build
	log "generating ${DATASET[*]} in $STORE (chunk $CHUNK_SIZE)"
	mkdir -p "$(dirname "$STORE")"
	"$BUILD/mkdata" gen "$STORE" "$CHUNK_SIZE" "${DATASET[@]}"
}

store_chunk_size() {
	[[ -f $STORE/.meta ]] || die "no store at $STORE; run ./run.sh $([[ $QUICK == 1 ]] && echo "--quick ")prepare"
	sed -n 's/^chunk_size=//p' "$STORE/.meta"
}

workload_prefix() {
	case $1 in
	seq1*) echo seq1_ ;; seq8) echo seq8_ ;; rand*) echo rand_ ;; smallfiles) echo small_ ;;
	esac
}

# Every file of a workload shares its prefix, so one glob covers it (4096 small files).
# uring-zc reads a request that spans two chunks from the .whole copy, so warm it too.
backing_files() { # workload mode
	local p=$STORE/$(workload_prefix "$1")
	case $2 in
	passthrough) printf '%s\n' "$p"*.whole ;;
	uring-zc) printf '%s\n' "$p"*.[0-9]* "$p"*.whole ;;
	*) printf '%s\n' "$p"*.[0-9]* ;;
	esac
}

fio_args() { # workload
	local common=(--ioengine=psync --direct=0 --group_reporting --readonly
		--allow_file_create=0 --output-format=json --invalidate=1)
	case $1 in
	seq1) echo "${common[@]} --name=seq1 --rw=read --bs=1M --numjobs=1 --filename=$MNT/seq1_0" ;;
	seq1-dio) echo "${common[@]} --name=seq1 --rw=read --bs=1M --numjobs=1 --filename=$MNT/seq1_0 --direct=1" ;;
	seq8) echo "${common[@]} --name=seq8 --rw=read --bs=1M --numjobs=8 --directory=$MNT --filename_format=seq8_\$jobnum" ;;
	rand4k-1j|rand4k-8j|rand4k-1j-dio|rand4k-8j-dio)
		local j=1 d=0
		[[ $1 == *8j* ]] && j=8
		[[ $1 == *dio ]] && d=1
		echo "${common[@]} --name=rand4k --rw=randread --bs=4k --numjobs=$j --filename=$MNT/rand_0 --time_based --runtime=$RUNTIME --norandommap --randrepeat=0 --direct=$d" ;;
	rand128k-aio) echo "${common[@]} --name=rand128k --rw=randread --bs=128k --ioengine=libaio --iodepth=32 --numjobs=1 --filename=$MNT/rand_0" ;;
	smallfiles)
		local sz=$((NSMALL * 64))
		echo "${common[@]} --name=small --rw=read --bs=64k --numjobs=1 --directory=$MNT --filename_format=small_\$filenum --nrfiles=$NSMALL --size=${sz}k --file_service_type=sequential --openfiles=1" ;;
	*) die "unknown workload $1" ;;
	esac
}

# ---------- one mount ----------

PID=""
STATS=""

# mountpoint(1) stats the path, which fails on a dead FUSE mount; read mountinfo instead.
is_mounted() { awk -v m="$MNT" '$5 == m { f = 1 } END { exit !f }' /proc/self/mountinfo; }

force_umount() {
	fusermount3 -u "$MNT" 2>/dev/null || umount "$MNT" 2>/dev/null ||
		fusermount3 -uz "$MNT" 2>/dev/null || umount -l "$MNT" 2>/dev/null || true
}

chunkfs_args() { # mode workload cache
	local mode=$1 args=(--store "$STORE" --chunk-size "$CS")
	case $mode in
	vmsplice-gift) args+=(--mode vmsplice --gift) ;;
	*) args+=(--mode "$mode") ;;
	esac
	[[ $mode == memcache && $3 == warm ]] && args+=(--preload "$(workload_prefix "$2")")
	echo "${args[@]} $CHUNKFS_OPTS"
}

mount_fs() { # mode workload cache dir
	local mode=$1 bin=$BUILD/chunkfs dir=$4
	[[ $mode == uring-zc ]] && bin=$BUILD/chunkfs-zc
	is_mounted && force_umount
	mkdir -p "$MNT"
	STATS=$dir/stats.json
	rm -f "$STATS" "$STATS.mark"
	# shellcheck disable=SC2046
	"$bin" $(chunkfs_args "$mode" "$2" "$3") --stats-json "$STATS" "$MNT" 2>"$dir/chunkfs.log" &
	PID=$!
	for ((i = 0; i < 1200; i++)); do
		if mountpoint -q "$MNT" && stat "$MNT/odd_0" >/dev/null 2>&1; then
			return 0
		fi
		kill -0 "$PID" 2>/dev/null || break
		sleep 0.1
	done
	cat "$dir/chunkfs.log" >&2
	umount_fs
	return 1
}

umount_fs() {
	is_mounted && force_umount
	if [[ -n $PID ]]; then
		for ((i = 0; i < 300; i++)); do kill -0 "$PID" 2>/dev/null || break; sleep 0.1; done
		kill -9 "$PID" 2>/dev/null || true
		wait "$PID" 2>/dev/null || true
	fi
	PID=""
}

verify_mount() { # dir
	local v=$1/verify.log
	{
		"$BUILD/mkdata" verify "$MNT/odd_0" odd_0 --random 300 &&
		"$BUILD/mkdata" verify "$MNT/odd_0" odd_0 --direct --random 300 --seed 7 &&
		"$BUILD/mkdata" verify "$MNT/small_1" small_1 --random 50 &&
		cmp "$MNT/odd_0" <(for ((i = 0; ; i++)); do [[ -e $STORE/odd_0.$i ]] || break; cat "$STORE/odd_0.$i"; done) &&
		echo "cmp $MNT/odd_0 against its chunks: ok"
	} >"$v" 2>&1
}

bdi_of_mount() {
	local majmin
	majmin=$(awk -v m="$MNT" '$5 == m { print $3 }' /proc/self/mountinfo | tail -1)
	[[ -n $majmin ]] && echo "/sys/class/bdi/$majmin"
}

resident() { # workload mode -> "resident_bytes total_bytes" of its backing files
	if command -v fincore >/dev/null; then
		backing_files "$1" "$2" | xargs fincore -b -n -r -o RES,SIZE 2>/dev/null |
			awk '{r += $1; s += $2} END {printf "%d %d\n", r, s}'
	else
		echo "-1 -1"
	fi
}

# ---------- one cell ----------

run_cell() { # mode workload cache rep
	local mode=$1 wl=$2 cache=$3 rep=${4:-0} why
	if ! why=$(mode_available "$mode"); then
		log "skip $mode/$wl/$cache: $why"
		return 0
	fi
	[[ $cache == cold && $IS_ROOT != 1 ]] && { log "skip $mode/$wl/cold: needs root"; return 0; }
	local id="$wl.$cache.$mode.r$rep"
	local dir=$RAW/$id
	mkdir -p "$dir" "$(dirname "$RESULTS")"
	log "cell $id"

	# memcache --preload reads the chunks at mount time; keep that off the disk.
	[[ $cache == warm ]] && backing_files "$wl" "$mode" | xargs cat >/dev/null

	local status=ok err=""
	if ! mount_fs "$mode" "$wl" "$cache" "$dir"; then
		status=mount_failed
		err=$(grep -vE "^chunkfs: (mode=|WARNING)|^\{" "$dir/chunkfs.log" | tail -3 | tr '\n' ' ')
	elif ! verify_mount "$dir"; then
		status=verify_failed
		err=$(grep -m1 -iE "mismatch|short|error|differ|EOF" "$dir/verify.log" || head -1 "$dir/verify.log")
	fi

	if [[ $status == ok ]]; then
		local bdi
		bdi=$(bdi_of_mount)
		if [[ -n $READ_AHEAD_KB && -n $bdi && $IS_ROOT == 1 ]]; then
			echo "$READ_AHEAD_KB" >"$bdi/read_ahead_kb"
		fi
		[[ -n $bdi && -r $bdi/read_ahead_kb ]] && cat "$bdi/read_ahead_kb" >"$dir/read_ahead_kb"
		if [[ $cache == cold ]]; then
			drop_caches
		else
			backing_files "$wl" "$mode" | xargs cat >/dev/null
		fi
		resident "$wl" "$mode" >"$dir/resident.before"
		kill -USR1 "$PID"
		for ((i = 0; i < 100; i++)); do [[ -e $STATS.mark ]] && break; sleep 0.05; done
		cat /proc/stat >"$dir/procstat.before"
		cat /proc/diskstats >"$dir/diskstats.before"
		cat "/proc/$PID/io" >"$dir/pio.before" 2>/dev/null || true
		# shellcheck disable=SC2046
		if ! "$BUILD/fio" $(fio_args "$wl") >"$dir/fio.json" 2>"$dir/fio.err"; then
			status=fio_failed
			err=$(head -c 300 "$dir/fio.err")
		fi
		cat /proc/stat >"$dir/procstat.after"
		cat /proc/diskstats >"$dir/diskstats.after"
		cat "/proc/$PID/io" >"$dir/pio.after" 2>/dev/null || true
		kill -USR2 "$PID"
		for ((i = 0; i < 100; i++)); do [[ -s $STATS ]] && break; sleep 0.05; done
		resident "$wl" "$mode" >"$dir/resident.after"
	fi
	umount_fs

	python3 "$HERE/record.py" "$dir" \
		mode="$mode" workload="$wl" cache="$cache" rep="$rep" status="$status" error="$err" \
		store="$STORE" chunk_size="$CS" chunkfs_args="$(chunkfs_args "$mode" "$wl" "$cache")" \
		fio_args="$(fio_args "$wl")" quick="$QUICK" runtime="$RUNTIME" \
		read_ahead_kb_set="$READ_AHEAD_KB" >>"$RESULTS"
	tail -1 "$RESULTS" | python3 "$HERE/summarize.py" --line >&2 || true

	if [[ $status == verify_failed ]]; then
		log "VERIFICATION FAILED for $mode ($err); see $dir/verify.log"
		[[ $KEEP_GOING == 1 ]] || exit 3
	fi
}

list_or_all() { # "a,b" default...
	local v=$1; shift
	if [[ -n $v ]]; then tr ',' '\n' <<<"$v"; else printf '%s\n' "$@"; fi
}

cmd_cell() {
	(($# >= 3)) || die "usage: run.sh cell MODE WORKLOAD warm|cold [REP]"
	need_build
	CS=$(store_chunk_size)
	run_cell "$@"
	log "results: $RESULTS"
}

cmd_matrix() {
	need_build
	CS=$(store_chunk_size)
	local caches=(); mapfile -t caches < <(list_or_all "$CACHES" warm cold)
	local modes=(); mapfile -t modes < <(list_or_all "$MODES" "${ALL_MODES[@]}")
	local wls=(); mapfile -t wls < <(list_or_all "$WORKLOADS" "${ALL_WORKLOADS[@]}")
	local m why
	for m in "${modes[@]}"; do
		why=$(mode_available "$m") || log "mode $m unavailable: $why"
	done
	log "matrix: ${#wls[@]} workloads x ${#modes[@]} modes x ${caches[*]} x $REPEATS -> $RESULTS"
	local rep wl cache
	for ((rep = 0; rep < REPEATS; rep++)); do
		for wl in "${wls[@]}"; do
			for cache in "${caches[@]}"; do
				for m in "${modes[@]}"; do
					run_cell "$m" "$wl" "$cache" "$rep"
				done
			done
		done
	done
	log "done: $RESULTS"
	python3 "$HERE/summarize.py" "$RESULTS"
}

cmd_smoke() {
	need_build
	CS=$(store_chunk_size)
	local modes=(); mapfile -t modes < <(list_or_all "$MODES" "${ALL_MODES[@]}")
	local wls=(); mapfile -t wls < <(list_or_all "$WORKLOADS" seq1 rand4k-1j-dio)
	local m wl
	for m in "${modes[@]}"; do
		for wl in "${wls[@]}"; do
			run_cell "$m" "$wl" warm 0
		done
	done
	python3 "$HERE/summarize.py" "$RESULTS"
}

trap 'umount_fs' EXIT
case ${1:-} in
prepare) cmd_prepare ;;
cell) shift; cmd_cell "$@" ;;
matrix) cmd_matrix ;;
smoke) cmd_smoke ;;
*) die "usage: run.sh [options] prepare|cell|matrix|smoke (see --help)" ;;
esac
