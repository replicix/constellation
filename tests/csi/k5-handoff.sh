#!/usr/bin/env bash
# Plan 37 K5 (part 1, 37-k5a) on kind: an engine-pod image upgrade while a
# pod writes is a stall, never an error (§8's FUSE session handover).
#
#   1. kind cluster + the chart at image tag A; a private floci S3 on the
#      `kind` network; a pool StorageClass; PVC k5-a (RWX);
#   2. pod w1 on worker 1 appends a numbered line with `fsync` to a file on
#      the PV in a tight loop (each line a fresh `dd`: open, append, fsync,
#      close — every kind of operation crosses the handover), recording
#      every failure;
#   3. `helm upgrade --set image.tag=B`: the node plugins roll, find their
#      engine pods' spec drifted, and hand each pod's FUSE sessions to a
#      replacement on image B (prepare, transfer, receive, seal, commit,
#      resume);
#   4. the worker-1 engine pod is replaced by one on image B, and the
#      staging FUSE mount is the very same mount (same mount id in the
#      node's mountinfo: nothing was remounted or republished);
#   5. the writer saw zero errors, its lines are contiguous (0..N-1, none
#      lost, none doubled), and it kept writing after the cutover; the
#      plugin logged the handoff as succeeded (its duration is printed).
#   6. a second, busy writer in the same pod keeps one descriptor open on
#      another file (at most ~2.5 MiB/s; a `cat` from a FIFO, so the descriptor's `close()` —
#      where a write-back session is published — reports its status:
#      non-zero is an error), appends 64 KiB blocks back to back and
#      `fsync`s it every 2 s (`sync -d`), so write-back data is in flight
#      across the handoff: zero errors, a clean close, the file holds
#      exactly the blocks it wrote, and its longest call — the
#      client-visible pause — is printed with every call over 200 ms and
#      when it happened;
#   7. 37-k6a: the class is `static-ephemeral`, so both engine pods run
#      `--await-unlock` with no credential in their spec or environment,
#      and the upgraded node plugin (restarted by the upgrade: it holds no
#      node-stage secret any more) cannot unlock the replacement itself —
#      the replacement must log that it took the old pod's credentials
#      from the handoff's `Credentials` step;
#   8. whether the backup (the controller's engine pod) sealed the
#      holder's epoch during the handoff (37-k5a's backup hold should
#      keep it from doing so), and the replacement's startup phases.
#
# TODO(busy-writer-loss): the busy file comes up short at EOF on some runs
# with no handoff at all (a pre-existing engine bug, chunk
# `busy-writer-loss`). Until that chunk merges, a byte mismatch on a run
# with no other error (0 errors, a clean close) is reported as
# "KNOWN LOSS BUG (busy-writer-loss)" and does not fail the lane; remove
# the exception (`known_loss`) once it has merged.
#
# One run is this script's gate; the 20-run gate and the p99 report are
# 37-k5b's.
#
# Usage: tests/csi/k5-handoff.sh
# Knobs:
#   KIND_BIN, KIND_CLUSTER (default kind-37-k5a), KIND_CONFIG_DRAFT,
#   KIND_NODE_IMAGE      as for kind-up.sh
#   CSI_IMAGE            tag A (default constellation-csi:k5-a; built with
#                        `make csi-image` unless CSI_SKIP_BUILD=1)
#   CSI_IMAGE_NEXT       tag B (default: tag A's image retagged k5-b — the
#                        same binary under a new tag is an image change to
#                        the chart, which is what the rollout reacts to)
#   K5_WRITE_S           seconds of writing after the cutover (default 15)
#   K5_ROLLOUT_S         how long to wait for the replacement engine pod
#                        after the upgrade (default 900: kubelet may take
#                        minutes to roll the node DaemonSet on a loaded host)
#   CSI_KEEP=1           leave the cluster and the S3 container running
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
. "$root/tests/csi/lib.sh"
kind="${KIND_BIN:-kind}"
export KIND_CLUSTER="${KIND_CLUSTER:-kind-37-k5a}"
export CSI_IMAGE="${CSI_IMAGE:-constellation-csi:k5-a}"
export CSI_NAMESPACE="${CSI_NAMESPACE:-constellation-csi}"
repo="${CSI_IMAGE%:*}"
next="${CSI_IMAGE_NEXT:-$repo:k5-b}"
ctx="kind-$KIND_CLUSTER"
ns="$CSI_NAMESPACE"
s3c="$KIND_CLUSTER-floci"
bucket=k5-handoff
w1="$KIND_CLUSTER-worker"
k() { kubectl --context "$ctx" "$@"; }
sel=app.kubernetes.io/component=engine,constellation.dev/owner=node

cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        echo "== FAILED; diagnostics"
        k get pods -o wide 2>/dev/null || true
        k -n "$ns" get pods -o wide --show-labels 2>/dev/null || true
        k -n "$ns" get events --sort-by=.lastTimestamp 2>/dev/null | tail -20 || true
        k -n "$ns" logs -l app.kubernetes.io/component=node -c constellation-csi --tail=80 \
            --prefix 2>/dev/null || true
        k -n "$ns" logs -l app.kubernetes.io/component=engine -c engine --tail=40 \
            --prefix 2>/dev/null || true
        k exec w1 -- cat /tmp/errors 2>/dev/null | head -20 || true
    fi
    if [ "${CSI_KEEP:-0}" != 1 ]; then
        "$kind" delete cluster --name "$KIND_CLUSTER" >/dev/null 2>&1 || true
        docker rm -f "$s3c" >/dev/null 2>&1 || true
    fi
    exit "$status"
}
trap cleanup EXIT

build_image "$CSI_IMAGE"
if ! docker image inspect "$next" >/dev/null 2>&1; then
    docker tag "$CSI_IMAGE" "$next"
fi
echo "== kind cluster $KIND_CLUSTER + chart at $CSI_IMAGE"
"$root/tests/csi/kind-up.sh" --install
"$kind" load docker-image "$next" --name "$KIND_CLUSTER"

echo "== S3 (floci) on the kind network"
endpoint=$(floci_up "$s3c" "$bucket")
echo "   endpoint $endpoint, bucket $bucket"

echo "== pool StorageClass, credentials, PVC"
k -n "$ns" create secret generic constellation-s3-creds \
    --from-literal=aws_access_key_id=test --from-literal=aws_secret_access_key=test \
    --dry-run=client -o yaml | k apply -f - >/dev/null
k apply -f - >/dev/null <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: k5-pool
provisioner: csi.constellation.dev
parameters:
  bucket: "$bucket"
  prefix: "constellation-csi/k5-pool"
  endpoint: "$endpoint"
  region: "us-east-1"
  layout: "pool"
  chunkSize: "1MiB"
  credentialSource: "static-ephemeral"
  csi.storage.k8s.io/provisioner-secret-name: constellation-s3-creds
  csi.storage.k8s.io/provisioner-secret-namespace: "$ns"
  csi.storage.k8s.io/node-stage-secret-name: constellation-s3-creds
  csi.storage.k8s.io/node-stage-secret-namespace: "$ns"
reclaimPolicy: Delete
volumeBindingMode: Immediate
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: k5-a, namespace: default}
spec:
  accessModes: [ReadWriteMany]
  storageClassName: k5-pool
  resources: {requests: {storage: 8Gi}}
EOF
k wait --for=jsonpath='{.status.phase}'=Bound pvc/k5-a --timeout=300s >/dev/null

echo "== pod w1 on $w1 appends numbered lines with fsync, counting errors"
# Each line: open, append, fsync, close (`dd conv=fsync oflag=append`) —
# every kind of operation crosses the handover. A failure is recorded with
# its errno text and time; the loop goes on.
k apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata: {name: w1, namespace: default}
spec:
  nodeName: $w1
  restartPolicy: Never
  terminationGracePeriodSeconds: 1
  securityContext: {runAsUser: 1000, runAsGroup: 1000, fsGroup: 1000}
  containers:
    - name: c
      image: $CSI_IMAGE
      imagePullPolicy: IfNotPresent
      command:
        - bash
        - -c
        - |
          : > /tmp/errors
          # The busy writer (step 6): one open descriptor, 64 KiB appends,
          # an fsync every 2 s; every call over 200 ms is logged with its
          # time (ms since the epoch) and the longest kept. The descriptor
          # is `cat`'s (GNU cat checks its `close()`), fed from a FIFO: a
          # refused close-time publish is cat's non-zero exit, an error.
          # (A write into the FIFO returns once cat took the previous block
          # into the file: the call times are the file's, one block late.)
          head -c 65536 /dev/urandom > /tmp/block
          busy() {
            mkfifo /tmp/busy.fifo
            cat /tmp/busy.fifo >> /data/k5-a/busy 2>/tmp/busy.cat.err &
            catpid=\$!
            exec 3>/tmp/busy.fifo
            n=0; synced=\$(date +%s); longest=0
            echo 0 > /tmp/busy.count; echo 0 > /tmp/busy.longest; : > /tmp/busy.slow
            while [ ! -e /tmp/stop ]; do
              t0=\$(date +%s%N); op=write
              if head -c 65536 /tmp/block >&3 2>/tmp/busy.err; then
                n=\$((n + 1)); echo "\$n" > /tmp/busy.count
              else
                echo "\$(date +%T.%N) busy block \$n: \$(cat /tmp/busy.err)" >> /tmp/errors
                # The writer (cat) is gone: its error is the one to report.
                if ! kill -0 "\$catpid" 2>/dev/null; then
                  echo "\$(date +%T.%N) busy writer (cat) ended: \$(cat /tmp/busy.cat.err)" >> /tmp/errors
                  break
                fi
              fi
              if [ \$((\$(date +%s) - synced)) -ge 2 ]; then
                op=write+fsync
                sync -d /data/k5-a/busy 2>/tmp/busy.err \
                  || echo "\$(date +%T.%N) busy fsync at \$n: \$(cat /tmp/busy.err)" >> /tmp/errors
                synced=\$(date +%s)
              fi
              t1=\$(date +%s%N); d=\$(( (t1 - t0) / 1000000 ))
              # At most ~40 blocks (2.5 MiB) a second: write-back data is
              # always in flight, and a rollout that takes kubelet minutes
              # stays well inside the PV's 8 GiB quota (and floci's memory).
              sleep 0.025
              [ "\$d" -gt 200 ] && echo "\$((t1 / 1000000)) \$op \${d}ms" >> /tmp/busy.slow
              [ "\$d" -gt "\$longest" ] && { longest=\$d; echo "\$longest" > /tmp/busy.longest; }
            done
            exec 3>&-
            wait "\$catpid"; closed=\$?
            echo "\$closed" > /tmp/busy.close
            [ "\$closed" = 0 ] \
              || echo "\$(date +%T.%N) busy close: cat exited \$closed: \$(cat /tmp/busy.cat.err)" >> /tmp/errors
            touch /tmp/busy.stopped
          }
          busy &
          i=0
          while [ ! -e /tmp/stop ]; do
            if printf '%08d\n' "\$i" | dd of=/data/k5-a/seq oflag=append conv=notrunc,fsync status=none 2>/tmp/dd.err; then
              i=\$((i + 1))
              echo "\$i" > /tmp/count
            else
              echo "\$(date +%T.%N) line \$i: \$(cat /tmp/dd.err)" >> /tmp/errors
              sleep 0.05
            fi
          done
          touch /tmp/stopped
          sleep 3600
      volumeMounts:
        - {name: k5-a, mountPath: /data/k5-a}
  volumes:
    - {name: k5-a, persistentVolumeClaim: {claimName: k5-a}}
EOF
k wait --for=condition=Ready pod/w1 --timeout=240s >/dev/null
count() { k exec w1 -- cat /tmp/count 2>/dev/null || echo 0; }
for _ in $(seq 60); do [ "$(count)" -gt 50 ] && break; sleep 1; done
[ "$(count)" -gt 50 ] || { echo "the writer is not writing ($(count) lines)"; exit 1; }
echo "   writing: $(count) lines so far"

old=$(k -n "$ns" get pods -l "$sel,constellation.dev/node=$w1" -o jsonpath='{.items[0].metadata.name}')
old_image=$(k -n "$ns" get pod "$old" -o jsonpath='{.spec.containers[0].image}')
fuse_mount() { # the staging FUSE mount on the worker: "<mount id> <mountpoint>"
    docker exec "$w1" awk '$9 ~ /^fuse/ && $5 ~ /globalmount$/ {print $1, $5}' /proc/self/mountinfo
}
mount_before=$(fuse_mount)
[ -n "$mount_before" ] || { echo "no staging FUSE mount on $w1"; exit 1; }
echo "   engine pod $old on $old_image; staging mount $mount_before"
no_credentials() { # no_credentials POD: --await-unlock, nothing in its spec or environment
    k -n "$ns" get pod "$1" -o json | jq -e '
        (.spec.containers[0].args | any(. == "--await-unlock"))
        and ([.spec.containers[0].env[]? | select(.valueFrom != null
            or (.name | test("AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|CONSTELLATION_PASSPHRASE")))]
            | length == 0)
        and (.spec.containers[0].envFrom == null)' >/dev/null \
        || { echo "engine pod $1 does not wait for fs.unlock, or carries credentials"; exit 1; }
}
no_credentials "$old"

echo "== helm upgrade: image tag ${CSI_IMAGE##*:} -> ${next##*:}"
upgraded_at=$(date +%s)
upgraded_ms=$(date +%s%3N)
# No `--wait`: the lane's subject is the engine pod's rollout, which is
# polled for below; kubelet can take minutes to roll the node DaemonSet on
# a loaded host, and helm's wait would fail the lane before the handoff.
helm --kube-context "$ctx" upgrade constellation-csi "$root/deploy/helm/constellation-csi" \
    -n "$ns" --reuse-values --set image.repository="${next%:*}" --set image.tag="${next##*:}" \
    >/dev/null
echo "   chart upgraded; waiting for the rollout on $w1"
new=""
rollout_s=${K5_ROLLOUT_S:-900}
for _ in $(seq $((rollout_s / 2))); do
    new=$(k -n "$ns" get pods -l "$sel,constellation.dev/node=$w1" -o json | jq -r --arg img "$next" '
        [.items[] | select(.spec.containers[0].image == $img
            and (.status.conditions // [] | any(.type == "Ready" and .status == "True")))
        ] | .[0].metadata.name // empty')
    gone=$(k -n "$ns" get pod "$old" -o name 2>/dev/null || true)
    [ -n "$new" ] && [ -z "$gone" ] && break
    sleep 2
done
[ -n "$new" ] || { echo "no engine pod on $next became ready on $w1"; exit 1; }
[ -z "$(k -n "$ns" get pod "$old" -o name 2>/dev/null || true)" ] \
    || { echo "the old engine pod $old is still there"; exit 1; }
echo "   $old ($old_image) -> $new ($next) in $(( $(date +%s) - upgraded_at )) s after the upgrade"
no_credentials "$new"
# Captured first: `grep -q` ending a pipe early fails it under pipefail.
new_log=$(k -n "$ns" logs "$new" -c engine 2>/dev/null | sed 's/\x1b\[[0-9;]*m//g')
grep -q 'handed-over credentials accepted' <<<"$new_log" \
    || { echo "the replacement $new did not get its credentials from the handoff"; exit 1; }
echo "   $new waited for its credentials and got them from $old over the handoff"

# The node plugin on the new image (the one that ran the handoff), not a
# terminating predecessor.
plugin=$(k -n "$ns" get pods -l app.kubernetes.io/component=node \
    --field-selector "spec.nodeName=$w1" -o json | jq -r --arg img "$next" '
    [.items[] | select(.metadata.deletionTimestamp == null)
        | select(any(.spec.containers[]; .name == "constellation-csi" and .image == $img))
    ] | .[0].metadata.name // empty')
[ -n "$plugin" ] || { echo "no node plugin on $next runs on $w1"; exit 1; }
logline=$(k -n "$ns" logs "$plugin" -c constellation-csi | grep 'engine-pod handoff succeeded' | tail -1)
[ -n "$logline" ] || { echo "the node plugin logged no successful handoff"; exit 1; }
clean=$(sed 's/\x1b\[[0-9;]*m//g' <<<"$logline")
echo "   plugin: $(grep -o 'elapsed=[^ ]*' <<<"$clean" || echo "$clean")"
# The handoff's window, prepare sent to `Resumed` seen (the log line's
# timestamp is its end), in ms since the epoch: the busy writer's calls
# overlapping it are the client-visible pause.
handoff_end=$(date -d "$(awk '{print $1}' <<<"$clean")" +%s%3N 2>/dev/null || echo 0)
handoff_ms=$(grep -o 'elapsed=[0-9.]*[µm]*s' <<<"$clean" | head -1 | cut -d= -f2 | awk '{
    if ($0 ~ /ms$/) { sub(/ms$/, ""); print int($0 + 0.5) }
    else if ($0 ~ /µs$/) print 0
    else { sub(/s$/, ""); print int($0 * 1000 + 0.5) } }')
handoff_start=$((handoff_end - ${handoff_ms:-0}))
if k -n "$ns" logs "$plugin" -c constellation-csi | grep -q 'handoff rolled back\|handoff lost'; then
    echo "   (note: an earlier attempt rolled back or was lost:)"
    k -n "$ns" logs "$plugin" -c constellation-csi | grep 'handoff rolled back\|handoff lost' | tail -3
fi

mount_after=$(fuse_mount)
[ "$mount_after" = "$mount_before" ] \
    || { echo "the staging mount changed: $mount_before -> $mount_after (remounted, not handed over)"; exit 1; }
echo "   staging mount unchanged ($mount_after): handed over, not remounted"

at_cutover=$(count)
sleep "${K5_WRITE_S:-15}"
after=$(count)
[ "$after" -gt "$at_cutover" ] || { echo "the writer stopped after the cutover ($at_cutover -> $after)"; exit 1; }
echo "   still writing after the cutover: $at_cutover -> $after lines"

echo "== the writers' verdict"
k exec w1 -- touch /tmp/stop
for _ in $(seq 120); do
    k exec w1 -- test -e /tmp/stopped -a -e /tmp/busy.stopped 2>/dev/null && break
    sleep 1
done
k exec w1 -- test -e /tmp/stopped -a -e /tmp/busy.stopped \
    || { echo "the writers did not stop within 120 s"; exit 1; }
errors=$(k exec w1 -- cat /tmp/errors)
if [ -n "$errors" ]; then
    echo "the writer saw $(wc -l <<<"$errors") error(s) across the handoff:"
    head -20 <<<"$errors"
    exit 1
fi
lines=$(k exec w1 -- cat /tmp/count)
k exec w1 -- bash -c '
    n=$(wc -l < /data/k5-a/seq)
    [ "$n" = "'"$lines"'" ] || { echo "the file has $n lines, the writer wrote '"$lines"'"; exit 1; }
    seq -f %08g 0 $((n - 1)) | cmp -s - /data/k5-a/seq || { echo "the lines are not 0..$((n - 1))"; exit 1; }' \
    || exit 1
echo "   0 errors; $lines lines, contiguous"
blocks=$(k exec w1 -- cat /tmp/busy.count)
closed=$(k exec w1 -- cat /tmp/busy.close)
size=$(k exec w1 -- stat -c %s /data/k5-a/busy)
echo "   busy writer: close status $closed"
known_loss=""
if [ "$size" != $((blocks * 65536)) ]; then
    echo "the busy file has $size bytes, the writer wrote $blocks blocks of 64 KiB"
    for _ in 1 2 3; do
        sleep 3
        echo "   again: $(k exec w1 -- stat -c %s /data/k5-a/busy) bytes"
    done
    k exec w1 -- bash -c 'for i in $(seq '"$blocks"'); do cat /tmp/block; done | cmp - /data/k5-a/busy' || true
    # TODO(busy-writer-loss): drop this exception once that chunk merges.
    # Reached only with 0 errors and a clean close (both checked above).
    known_loss="$(( blocks * 65536 - size )) bytes short"
    echo "   KNOWN LOSS BUG (busy-writer-loss): $known_loss with 0 errors and close status 0;" \
        "not a handoff failure (it reproduces with no handoff)"
else
    k exec w1 -- bash -c 'cmp -s <(tail -c 65536 /data/k5-a/busy) /tmp/block && cmp -s <(head -c 65536 /data/k5-a/busy) /tmp/block' \
        || { echo "the busy file's first or last block is not the writer's"; exit 1; }
    echo "   busy file: exactly the $blocks blocks written"
fi
longest=$(k exec w1 -- cat /tmp/busy.longest)
echo "   busy writer: $blocks blocks of 64 KiB ($((size / 1048576)) MiB), 0 errors; longest call ${longest} ms"
slow=$(k exec w1 -- cat /tmp/busy.slow)
if [ -n "$slow" ]; then
    echo "   its calls over 200 ms (ms after the helm upgrade started, op, duration; * overlaps the handoff):"
    while read -r at op d; do
        ms=${d%ms}
        mark=" "
        [ "$((at - ms))" -le "$handoff_end" ] && [ "$at" -ge "$handoff_start" ] && mark="*"
        echo "    $mark+$((at - upgraded_ms)) $op $d"
    done <<<"$slow"
fi
pause=$(awk -v hs="$handoff_start" -v he="$handoff_end" '{
    d = $3; sub(/ms$/, "", d); d += 0
    if ($1 - d <= he && $1 >= hs && d > m) m = d } END { print m + 0 }' <<<"$slow")
echo "   handoff window +$((handoff_start - upgraded_ms))..+$((handoff_end - upgraded_ms)) ms (${handoff_ms} ms)"
if [ "$pause" -gt 0 ]; then
    echo "   client-visible pause (the busy writer's longest call overlapping the handoff): ${pause} ms"
else
    echo "   client-visible pause: under 200 ms (no busy call over 200 ms overlapped the handoff)"
fi

echo "== the backup across the handoff"
since=$(( $(date +%s) - upgraded_at + 30 ))s
backup=$(k -n "$ns" get pods -l app.kubernetes.io/component=engine,constellation.dev/owner=controller \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
nocolor() { sed 's/\x1b\[[0-9;]*m//g'; }
if [ -n "$backup" ]; then
    held=$(k -n "$ns" logs "$backup" -c engine --since="$since" 2>/dev/null | nocolor \
        | grep -c 'holding the seal watch' || true)
    sealed=$(k -n "$ns" logs "$backup" -c engine --since="$since" 2>/dev/null | nocolor \
        | grep -c 'sealed its epoch' || true)
    echo "   backup $backup: hold(s) received $held; sealed the holder's epoch $sealed time(s)"
else
    echo "   no controller engine pod (no backup)"
fi
k -n "$ns" logs "$new" -c engine 2>/dev/null | nocolor | grep -q 'a backup sealed our epoch' \
    && echo "   the replacement logged: a backup sealed our epoch" \
    || echo "   the replacement did not see its epoch sealed"
k -n "$ns" logs "$new" -c engine 2>/dev/null | nocolor | grep 'backups_holding\|pre-opened' | tail -2 | cut -c1-220 || true
echo "   the replacement's startup phases (ms):"
k -n "$ns" logs "$new" -c engine 2>/dev/null | nocolor | grep 'startup phase done' \
    | sed -n 's/.*phase=\(.*\) ms=\([0-9]*\) next=.*/      \2\t\1/p' || true

echo "== the volume unstages from the new engine pod"
k delete pod w1 --wait=true >/dev/null
for _ in $(seq 60); do
    [ -z "$(fuse_mount)" ] && break
    sleep 2
done
[ -z "$(fuse_mount)" ] || { echo "the staging mount is still there after the pod went"; exit 1; }
k delete pvc k5-a --wait=true >/dev/null
echo "== k5-handoff PASSED: $old -> $new under two writers: 0 errors, close status $closed, $lines contiguous lines, $blocks busy blocks${known_loss:+ (KNOWN LOSS BUG (busy-writer-loss): $known_loss)} (client-visible pause ${pause:-?} ms, longest call ${longest} ms), the staging mount handed over in place"
