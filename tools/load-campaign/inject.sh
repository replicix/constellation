#!/usr/bin/env bash
# Host-load injectors for processes the harness did not spawn (the chaos
# soak, a hand-run cluster), the shell twins of crates/harness/src/load.rs,
# plus the runner's dm-delay disk. Root via `sudo -n` where needed.
#
#   inject.sh cpu <pid> <pct>            put <pid> in its own cgroup at cpu.max
#                                        <pct>% of one CPU (100 = one CPU)
#   inject.sh cpu-bursty <pid> [s]       toggle that cgroup 100% / 2% every s
#                                        (default 3) until killed
#   inject.sh cpu-release <pid>          move <pid> back, remove its cgroup
#   inject.sh pause <pid> <ms>           SIGSTOP, sleep, SIGCONT
#   inject.sh hogs-start [stress-ng args] start stress-ng (default: the
#                                        campaign's cpu/vm/io/hdd mix) in its
#                                        own process group; prints its pid
#   inject.sh hogs-stop <pid>            kill that process group
#   inject.sh netem <pid> <netem args>   tc netem on <pid>'s UDP ports on lo
#   inject.sh netem-clear                remove the lo root qdisc this made
#   inject.sh store-stall <file> <ms> [pid]
#                                        arm a daemon's
#                                        CONSTELLATION_FAULT_STORE_STALL_FILE
#                                        for one write of <ms> (only the
#                                        daemon <pid>, if given)
#   inject.sh toxic <api> <proxy> <name> <type> <json-attributes>
#   inject.sh toxic-rm <api> <proxy> <name>
#                                        toxiproxy (e.g. api http://127.0.0.1:8474)
#   inject.sh dmdelay-up <dir> <MiB> <read_ms> <write_ms>
#                                        a loop file under <dir>, dm-delay over
#                                        it, ext4, mounted at <dir>/mnt (owned
#                                        by the caller); prints the mountpoint
#   inject.sh dmdelay-down <dir>         unmount and remove it all
#   inject.sh rotate <secs> <pid>...     the soak schedule: every <secs> pick one
#                                        of {cpu-bursty 60 s, pause of a random
#                                        boundary, hogs 60 s} for a random pid
#   inject.sh cleanup                    thaw daemons a dead run's injectors left
#                                        stopped, remove every cst-load-* cgroup of
#                                        a dead run (cst-load-sh when empty), the
#                                        lo prio qdisc (unconditionally: not on a
#                                        host where another campaign runs) and dm
#                                        devices named cst-load-*
#
# Every change is printed with a timestamp, for the run's triage log.
set -euo pipefail

CG=/sys/fs/cgroup
PREFIX=cst-load
as_root() { if [ "$(id -u)" = 0 ]; then "$@"; else sudo -n "$@"; fi; }
note() { echo "$(date -u +%FT%T.%3NZ) inject: $*"; }

cg_of() { echo "$CG/$PREFIX-sh/$1"; }

udp_ports() {
  local pid=$1 inodes
  inodes=$(ls -l "/proc/$pid/fd" 2>/dev/null | sed -n 's/.*socket:\[\([0-9]*\)\].*/\1/p' | sort -u)
  [ -n "$inodes" ] || return 0
  for t in udp udp6; do
    awk -v want="$(echo "$inodes" | tr '\n' ' ')" '
      BEGIN { n = split(want, w, " "); for (i = 1; i <= n; i++) ok[w[i]] = 1 }
      NR > 1 && ($10 in ok) { split($2, a, ":"); p = strtonum("0x" a[2]); if (p) print p }
    ' "/proc/$pid/net/$t"
  done | sort -un
}

cmd=${1:-}
shift || true
case "$cmd" in
cpu)
  pid=$1 pct=$2 g=$(cg_of "$1")
  [ "$pct" -ge 1 ] 2>/dev/null || { echo "cpu: <pct> must be a positive integer" >&2; exit 2; }
  as_root sh -c "set -e; mkdir -p $CG/$PREFIX-sh; grep -qw cpu $CG/$PREFIX-sh/cgroup.subtree_control || echo +cpu > $CG/$PREFIX-sh/cgroup.subtree_control; mkdir -p $g; echo '$((pct * 1000)) 100000' > $g/cpu.max; echo $pid > $g/cgroup.procs"
  note "cpu $pid at $pct% of one CPU ($g)"
  ;;
cpu-bursty)
  pid=$1 period=${2:-3} g=$(cg_of "$1")
  [ -d "$g" ] || "$0" cpu "$pid" 100
  note "cpu-bursty $pid every ${period}s"
  while kill -0 "$pid" 2>/dev/null; do
    as_root sh -c "echo 'max 100000' > $g/cpu.max"; sleep "$period"
    as_root sh -c "echo '2000 100000' > $g/cpu.max"; sleep "$period"
  done
  ;;
cpu-release)
  pid=$1 g=$(cg_of "$1")
  own=$(sed -n 's/^0:://p' /proc/self/cgroup)
  as_root sh -c "for p in \$(cat $g/cgroup.procs 2>/dev/null); do echo \$p > $CG$own/cgroup.procs 2>/dev/null || echo \$p > $CG/cgroup.procs; done; rmdir $g 2>/dev/null; rmdir $CG/$PREFIX-sh 2>/dev/null; true"
  note "cpu-release $pid"
  ;;
pause)
  pid=$1 ms=$2
  note "pause $pid for ${ms}ms"
  # Thaw it whatever ends this script.
  trap 'kill -CONT "$pid" 2>/dev/null' EXIT
  kill -STOP "$pid"
  sleep "$(awk -v m="$ms" 'BEGIN { printf "%.3f", m / 1000 }')"
  kill -CONT "$pid"
  note "resumed $pid"
  ;;
hogs-start)
  args=("$@")
  if [ ${#args[@]} = 0 ]; then
    args=(--cpu "$(nproc)" --cpu-load 90 --vm 4 --vm-bytes 60% --io 4 --hdd 2 --hdd-bytes 256m)
  fi
  setsid stress-ng --timeout 86400s --quiet "${args[@]}" </dev/null >/dev/null 2>&1 &
  note "hogs pid $! (${args[*]})"
  echo $!
  ;;
hogs-stop)
  kill -KILL -- "-$1" 2>/dev/null || kill -KILL "$1" 2>/dev/null || true
  note "hogs $1 stopped"
  ;;
netem)
  pid=$1; shift
  if ! as_root tc qdisc show dev lo | grep -q ' prio '; then
    as_root tc qdisc show dev lo | grep -q noqueue || { echo "lo has a root qdisc already" >&2; exit 1; }
    as_root tc qdisc add dev lo root handle 1: prio bands 16 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1
  fi
  band=$(( $(as_root tc qdisc show dev lo | grep -c netem) + 4 ))
  hex=$(printf %x "$band")
  as_root tc qdisc add dev lo parent "1:$hex" handle "${hex}0:" netem "$@"
  for p in $(udp_ports "$pid"); do
    for d in sport dport; do
      as_root tc filter add dev lo parent 1: protocol ip prio 1 u32 match ip protocol 17 0xff match ip "$d" "$p" 0xffff flowid "1:$hex"
      as_root tc filter add dev lo parent 1: protocol ipv6 prio 2 u32 match ip6 protocol 17 0xff match ip6 "$d" "$p" 0xffff flowid "1:$hex"
    done
    note "netem band $band ($*) on $pid UDP port $p"
  done
  ;;
netem-clear)
  if as_root tc qdisc show dev lo | grep -q ' prio '; then as_root tc qdisc del dev lo root; fi
  note "netem cleared"
  ;;
store-stall)
  # Whole, by a rename: a daemon reading a prefix stalls for the wrong time.
  echo "$2 once${3:+ pid=$3}" > "$1.tmp" && mv -f "$1.tmp" "$1"
  note "store stall of $2 ms armed at $1"
  ;;
toxic)
  api=$1 proxy=$2 name=$3 type=$4 attrs=$5
  curl -fsS -X POST "$api/proxies/$proxy/toxics" -H 'content-type: application/json' \
    -d "{\"name\":\"$name\",\"type\":\"$type\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":$attrs}" >/dev/null
  note "toxic $name ($type $attrs) on $proxy"
  ;;
toxic-rm)
  curl -fsS -X DELETE "$1/proxies/$2/toxics/$3" >/dev/null || true
  note "toxic $3 removed from $2"
  ;;
dmdelay-up)
  dir=$1 mib=$2 rd=$3 wr=$4 name=$PREFIX-$$
  mkdir -p "$dir/mnt"
  truncate -s "${mib}M" "$dir/disk.img"
  loop=$(as_root losetup --find --show "$dir/disk.img")
  sectors=$(as_root blockdev --getsz "$loop")
  as_root dmsetup create "$name" --table "0 $sectors delay $loop 0 $rd $loop 0 $wr"
  as_root mkfs.ext4 -q "/dev/mapper/$name"
  as_root mount "/dev/mapper/$name" "$dir/mnt"
  as_root chown "$(id -u):$(id -g)" "$dir/mnt"
  printf '%s %s\n' "$name" "$loop" > "$dir/dmdelay.state"
  note "dm-delay $name over $loop (${rd}ms reads, ${wr}ms writes) at $dir/mnt"
  echo "$dir/mnt"
  ;;
dmdelay-down)
  dir=$1
  read -r name loop < "$dir/dmdelay.state"
  as_root umount "$dir/mnt" 2>/dev/null || as_root umount -l "$dir/mnt" || true
  as_root dmsetup remove "$name" || true
  as_root losetup -d "$loop" || true
  rm -f "$dir/disk.img" "$dir/dmdelay.state"
  rmdir "$dir/mnt" 2>/dev/null || true
  note "dm-delay $name removed"
  ;;
rotate)
  secs=$1; shift
  pids=("$@")
  while :; do
    sleep "$secs"
    pid=${pids[$((RANDOM % ${#pids[@]}))]}
    kill -0 "$pid" 2>/dev/null || continue
    case $((RANDOM % 3)) in
    0) timeout 60 "$0" cpu-bursty "$pid" || true; "$0" cpu-release "$pid" ;;
    1) "$0" pause "$pid" "$(( (RANDOM % 4 + 1) * 1500 + RANDOM % 1000 ))" ;;
    2) h=$("$0" hogs-start | tail -1); sleep 60; "$0" hogs-stop "$h" ;;
    esac
  done
  ;;
cleanup)
  for f in "${TMPDIR:-/tmp}"/cst-load-stopped-*.pids; do
    [ -f "$f" ] || continue
    h=${f##*cst-load-stopped-}; h=${h%.pids}
    kill -0 "$h" 2>/dev/null && continue
    for p in $(cat "$f"); do
      if [ "$(awk '{print $3}' "/proc/$p/stat" 2>/dev/null)" = T ]; then
        kill -CONT "$p" && note "thawed $p (left stopped by run $h)"
      fi
    done
    rm -f "$f"
  done
  for g in "$CG"/$PREFIX-*; do
    [ -d "$g" ] || continue
    pid=${g##*-}
    if [ "$pid" != sh ] && kill -0 "$pid" 2>/dev/null; then continue; fi
    if [ "$pid" = sh ] && [ -n "$(cat "$g"/*/cgroup.procs 2>/dev/null)" ]; then continue; fi
    as_root sh -c "for c in $g/*/; do [ -d \"\$c\" ] || continue; for p in \$(cat \$c/cgroup.procs); do echo \$p > $CG/cgroup.procs; done; rmdir \$c; done; rmdir $g" || true
    note "removed $g"
  done
  if as_root tc qdisc show dev lo | grep -q ' prio '; then as_root tc qdisc del dev lo root; note "removed the lo prio qdisc"; fi
  rm -f "${TMPDIR:-/tmp}/cst-load-netem.marker"
  for d in $(as_root dmsetup ls 2>/dev/null | awk -v p="$PREFIX-" 'index($1, p) == 1 { print $1 }'); do
    as_root dmsetup remove "$d" || true
    note "removed dm device $d"
  done
  ;;
*)
  sed -n '2,/^set -euo/p' "$0" | sed '$d; s/^# \{0,1\}//'
  exit 2
  ;;
esac
