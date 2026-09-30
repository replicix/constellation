#!/usr/bin/env bash
# Run plan 38 Z0a's four handover variants on the running kernel (root only).
#
#   sudo ./run.sh [--variants "1 2 3 4"] [--repeats 3] [--gap 3] [--out results]
#
# Per run (results/<uname -r>/v<variant>-r<repeat>/):
#   1. process A mounts /mnt/hospike, serves FUSE over io_uring;
#   2. two clients (pinned to CPU 1 and CPU 5) read the probe file continuously,
#      every read verified, a status line every 100 ms (client{1,5}.log);
#   3. after 3 s, SIGUSR1 to A: the handover (see src/hospike.c);
#   4. B registers (variants 2, 4) --gap seconds after A is gone;
#      A "gap probe" (CPU 3) is issued 1 s after A is gone, before B registers;
#   5. "kick": one fresh probe read pinned to every CPU, 3 s budget each;
#   6. final state of every client and probe (running / D-state / exited),
#      /sys/fs/fuse/connections/<id>/waiting, dmesg;
#   7. teardown: SIGTERM the clients; anything that survives it is stuck in the
#      kernel, so the connection is aborted via fusectl (recorded).
# analyze.py turns each run dir into one JSON line in summary.jsonl.
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
BIN=$HERE/build/hospike
MNT=/mnt/hospike
VARIANTS="1 2 3 4"
REPEATS=3
GAP=3
OUT=$HERE/results
while [[ $# -gt 0 ]]; do
	case $1 in
	--variants) VARIANTS=$2; shift 2 ;;
	--repeats) REPEATS=$2; shift 2 ;;
	--gap) GAP=$2; shift 2 ;;
	--out) OUT=$2; shift 2 ;;
	*) echo "unknown arg $1" >&2; exit 2 ;;
	esac
done

[[ $EUID == 0 ]] || { echo "run as root" >&2; exit 2; }
[[ -x $BIN ]] || { echo "build first: ./build.sh" >&2; exit 2; }
echo Y >/sys/module/fuse/parameters/enable_uring
mountpoint -q /sys/fs/fuse/connections || mount -t fusectl none /sys/fs/fuse/connections
mkdir -p "$MNT"
NCPU=$(getconf _NPROCESSORS_CONF)
RES=$OUT/$(uname -r)
mkdir -p "$RES"
{
	echo "kernel=$(uname -r)"
	echo "os=$(. /etc/os-release; echo "$PRETTY_NAME")"
	echo "cpus=$NCPU possible=$(cat /sys/devices/system/cpu/possible)"
	echo "enable_uring=$(cat /sys/module/fuse/parameters/enable_uring)"
	echo "liburing=$(ls /usr/lib*/liburing.so.* /usr/lib/x86_64-linux-gnu/liburing.so.* 2>/dev/null | head -1)"
	echo "date=$(date -u +%FT%TZ)"
} >"$RES/host.txt"

now() { date +%s.%N; }
sget() { sed -n "s/^$2=//p" "$1" 2>/dev/null | head -1; }
pstate() { # pid -> "R", "S", "D", "Z" or "gone"
	[[ -r /proc/$1/stat ]] || { echo gone; return; }
	sed -E 's/^[0-9]+ \(.*\) ([A-Z]).*/\1/' "/proc/$1/stat" 2>/dev/null || echo gone
}
alive() { local s; s=$(pstate "$1"); [[ $s != gone && $s != Z ]]; }
wait_gone() { # pid seconds
	local end=$(($(date +%s) + $2))
	while alive "$1"; do (($(date +%s) >= end)) && return 1; sleep 0.05; done
	return 0
}
describe() { # pid -> "state wchan"
	if alive "$1"; then echo "$(pstate "$1") wchan=$(cat /proc/"$1"/wchan 2>/dev/null)"; else echo exited; fi
}
conn_id() { mountpoint -d "$MNT" 2>/dev/null | cut -d: -f2; }
waiting() { cat "/sys/fs/fuse/connections/$1/waiting" 2>/dev/null || echo none; }

cleanup_mount() {
	# a dead FUSE mount fails stat(2), so `mountpoint` would miss it: use /proc/mounts
	local c
	pkill -KILL -f "hospike (serve-a|client|probe)" 2>/dev/null
	if grep -q " $MNT fuse.hospike " /proc/mounts; then
		for c in /sys/fs/fuse/connections/*/; do echo 1 >"$c/abort" 2>/dev/null; done
		umount -l "$MNT" 2>/dev/null
	fi
	pkill -KILL -f "hospike serve-b" 2>/dev/null
	sleep 0.2
}

run_one() { # variant repeat
	local V=$1 R=$2 D=$RES/v$1-r$2 APID C1 C5 CONN BPID p cpu dm0 aborted=0
	local -a PPIDS=()
	rm -rf "$D"
	mkdir -p "$D"
	cleanup_mount
	dm0=$(dmesg | wc -l)

	"$BIN" serve-a --mnt "$MNT" --variant "$V" --out "$D" --gap "$GAP" 2>"$D/a.log" &
	APID=$!
	for _ in $(seq 100); do [[ $(sget "$D/a.stats" ring_ready) == 1 ]] && break; sleep 0.1; done
	if [[ $(sget "$D/a.stats" ring_ready) != 1 ]]; then
		echo "v$V r$R: A never got its ring ready, see $D/a.log" | tee "$D/FAILED"
		cleanup_mount
		return 1
	fi
	CONN=$(conn_id)
	echo "$CONN" >"$D/conn"
	waiting "$CONN" >"$D/waiting.start"

	taskset -c 1 "$BIN" client --file "$MNT/probe" --log "$D/client1.log" &
	C1=$!
	taskset -c 5 "$BIN" client --file "$MNT/probe" --log "$D/client5.log" &
	C5=$!
	sleep 3
	cp "$D/a.stats" "$D/a.stats.prehandover"
	now >"$D/t_usr1"
	kill -USR1 "$APID"
	wait_gone "$APID" 15 || echo "A still alive 15 s after SIGUSR1" >>"$D/notes"
	now >"$D/t_a_exit"
	sleep 1
	waiting "$CONN" >"$D/waiting.after_a"
	# gap probe: one read issued while nobody serves the ring (B registers, in
	# variants 2 and 4, --gap s after A is gone). Does B's REGISTER alone
	# dispatch it, or does it wait for another request on its queue (the kick)?
	now >"$D/t_gapprobe"
	taskset -c 3 "$BIN" probe --file "$MNT/probe" >"$D/probe.gap" 2>&1 &
	local GP=$!
	BPID=$(sget "$D/b.stats" pid)
	echo "${BPID:-none}" >"$D/b.pid"
	sleep $((GAP + 3))
	waiting "$CONN" >"$D/waiting.before_kick"
	cp "$D/b.stats" "$D/b.stats.before_kick" 2>/dev/null

	now >"$D/t_kick"
	for cpu in $(seq 0 $((NCPU - 1))); do
		taskset -c "$cpu" "$BIN" probe --file "$MNT/probe" >"$D/probe.cpu$cpu" 2>&1 &
		PPIDS[$cpu]=$!
	done
	sleep 3
	alive "$GP" && echo "HUNG $(describe "$GP")" >>"$D/probe.gap"
	for cpu in $(seq 0 $((NCPU - 1))); do
		p=${PPIDS[$cpu]}
		if alive "$p"; then echo "HUNG $(describe "$p")" >>"$D/probe.cpu$cpu"; fi
	done
	sleep 3
	{
		echo "client1 $(describe "$C1")"
		echo "client5 $(describe "$C5")"
		[[ -n $BPID ]] && echo "B $(describe "$BPID")"
	} >"$D/final.state"
	waiting "$CONN" >"$D/waiting.final"
	cp "$D/b.stats" "$D/b.stats.final" 2>/dev/null
	cp "$D/a.stats" "$D/a.stats.final" 2>/dev/null

	# teardown: whatever survives SIGTERM is waiting in the kernel
	kill -TERM "$C1" "$C5" "$GP" "${PPIDS[@]}" 2>/dev/null
	sleep 1
	local stuck=""
	for p in "$C1" "$C5" "$GP" "${PPIDS[@]}"; do alive "$p" && stuck+="$p:$(pstate "$p") "; done
	echo "after SIGTERM: ${stuck:-none}" >>"$D/final.state"
	if [[ -n $stuck ]]; then
		echo 1 >"/sys/fs/fuse/connections/$CONN/abort" 2>/dev/null && aborted=1
		sleep 1
		stuck=""
		for p in "$C1" "$C5" "$GP" "${PPIDS[@]}"; do alive "$p" && stuck+="$p:$(pstate "$p") "; done
		echo "after fusectl abort: ${stuck:-none}" >>"$D/final.state"
	fi
	echo "$aborted" >"$D/aborted"
	[[ -n $BPID ]] && kill -TERM "$BPID" 2>/dev/null
	umount -l "$MNT" 2>/dev/null
	echo 1 >"/sys/fs/fuse/connections/$CONN/abort" 2>/dev/null
	[[ -n $BPID ]] && { wait_gone "$BPID" 3 || kill -KILL "$BPID" 2>/dev/null; }
	cleanup_mount
	# no `wait`: a child stuck in the kernel would block it forever
	dmesg | tail -n +$((dm0 + 1)) >"$D/dmesg.txt"
	python3 "$HERE/analyze.py" "$D" | tee -a "$RES/summary.jsonl" | python3 "$HERE/analyze.py" --human
}

for R in $(seq 1 "$REPEATS"); do
	for V in $VARIANTS; do
		run_one "$V" "$R"
	done
done
echo "done: $RES/summary.jsonl"
