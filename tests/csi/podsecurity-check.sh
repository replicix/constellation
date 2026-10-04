#!/usr/bin/env bash
# Plan 37 K6 gate: a kube-bench-style check that the driver's pods hold
# exactly the privilege split of plan 37 §9 — no more, no less:
#
#   node plugin            the only privileged component: its constellation-csi
#                          container privileged (fuse_mount_fd, bind mounts) and
#                          root (uid 0), with Bidirectional propagation on its
#                          three host mounts; hostPaths exactly the plugin dir,
#                          the registration dir, the kubelet dir, hostRoot and
#                          /dev/fuse; its sidecars unprivileged (no escalation,
#                          every capability dropped, none added)
#   controller             non-root (65532), RuntimeDefault seccomp, every
#                          container unprivileged, read-only root, no hostPath
#   engine pods (both)     non-root 65532, RuntimeDefault seccomp, no token, the
#                          constellation-csi-engine ServiceAccount, no host
#                          namespace, no init container, one container under
#                          PodSecurity restricted's rules (no privilege, no
#                          escalation, ALL dropped, none added, read-only root)
#   controller-owned       emptyDir volumes only (PodSecurity restricted whole)
#   node-owned             hostPaths exactly <hostRoot>/{node-identity,sockets,policy}/<unit>
#                          of its own unit, type Directory, policy read-only
#
# and, across the namespace, that the privileged containers are exactly the
# node plugins' constellation-csi containers.
#
# Every check prints `[PASS]`/`[FAIL] <id> <what>` (kube-bench's shape) and the
# script exits non-zero on any FAIL. Each pod kind must be present: with
# PSC_STORAGE_CLASS set, the script first stages a volume of that class on a
# worker (a PVC and a pod in a namespace of its own, deleted afterwards), so
# both kinds of engine pod exist; otherwise a missing kind is a FAIL.
#
# Usage: tests/csi/podsecurity-check.sh
# Knobs:
#   KUBECONFIG, KUBE_CONTEXT   the cluster (kubectl's defaults otherwise)
#   CSI_NAMESPACE              the driver's namespace (default constellation-csi)
#   CSI_HOST_ROOT              the chart's hostRoot (default /var/lib/constellation-csi)
#   CSI_KUBELET_DIR            the chart's kubeletDir (default /var/lib/kubelet)
#   CSI_DRIVER_NAME            the chart's driverName (default csi.constellation.dev)
#   PSC_STORAGE_CLASS          a pool StorageClass to stage a volume of first
#   PSC_IMAGE                  the workload pod's image (default constellation-csi:dev)
set -euo pipefail

ns="${CSI_NAMESPACE:-constellation-csi}"
host_root="${CSI_HOST_ROOT:-/var/lib/constellation-csi}"
host_root="${host_root%/}"
kubelet_dir="${CSI_KUBELET_DIR:-/var/lib/kubelet}"
kubelet_dir="${kubelet_dir%/}"
driver="${CSI_DRIVER_NAME:-csi.constellation.dev}"
k() {
    if [ -n "${KUBE_CONTEXT:-}" ]; then
        kubectl --context "$KUBE_CONTEXT" "$@"
    else
        kubectl "$@"
    fi
}

pass=0
fail=0
check() { # check ID WHAT JQ-EXPR-ON-$doc
    local id="$1" what="$2" expr="$3" out
    if out=$(jq -e "$expr" <<<"$doc" 2>&1) && [ "$out" = true ]; then
        echo "[PASS] $id $what"
        pass=$((pass + 1))
    else
        echo "[FAIL] $id $what"
        fail=$((fail + 1))
    fi
}

work_ns=""
cleanup() {
    if [ -n "$work_ns" ]; then
        k delete namespace "$work_ns" --wait=true --timeout=180s >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

if [ -n "${PSC_STORAGE_CLASS:-}" ]; then
    work_ns="psc-$(date +%s)"
    worker=$(k get nodes -o json | jq -r '[.items[] | select(.metadata.labels["node-role.kubernetes.io/control-plane"] == null) | .metadata.name][0]')
    echo "== staging a volume of $PSC_STORAGE_CLASS on $worker (namespace $work_ns)"
    k create namespace "$work_ns" >/dev/null
    k apply -f - >/dev/null <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: psc, namespace: $work_ns}
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: $PSC_STORAGE_CLASS
  resources: {requests: {storage: 64Mi}}
---
apiVersion: v1
kind: Pod
metadata: {name: psc, namespace: $work_ns}
spec:
  nodeName: $worker
  terminationGracePeriodSeconds: 1
  securityContext: {runAsUser: 1000, runAsGroup: 1000, fsGroup: 1000}
  containers:
    - name: c
      image: ${PSC_IMAGE:-constellation-csi:dev}
      imagePullPolicy: IfNotPresent
      command: [sleep, infinity]
      volumeMounts: [{name: v, mountPath: /data}]
  volumes: [{name: v, persistentVolumeClaim: {claimName: psc}}]
EOF
    k -n "$work_ns" wait --for=condition=Ready pod/psc --timeout=300s >/dev/null
fi

pods=$(k -n "$ns" get pods -o json)
echo "== plan 37 §9 privilege split, namespace $ns"

# --- 1. node plugin -------------------------------------------------------
n=0
for name in $(jq -r '.items[] | select(.metadata.labels["app.kubernetes.io/component"] == "node") | .metadata.name' <<<"$pods"); do
    n=$((n + 1))
    doc=$(jq --arg n "$name" --arg root "$host_root" --arg kubelet "$kubelet_dir" --arg driver "$driver" \
        '.items[] | select(.metadata.name == $n) | . + {root: $root, kubelet: $kubelet, driver: $driver}' <<<"$pods")
    check "1.1" "$name: constellation-csi is privileged (CAP_SYS_ADMIN for fuse_mount_fd and bind mounts)" \
        '[.spec.containers[] | select(.name == "constellation-csi") | .securityContext.privileged] == [true]'
    check "1.2" "$name: Bidirectional propagation on exactly plugin-dir, kubelet-dir, host-root" \
        '[.spec.containers[] | select(.name == "constellation-csi") | .volumeMounts[] | select(.mountPropagation == "Bidirectional") | .name] | sort == ["host-root", "kubelet-dir", "plugin-dir"]'
    check "1.3" "$name: every sidecar unprivileged, no escalation, ALL dropped, nothing added" \
        '[.spec.containers[] | select(.name != "constellation-csi") | (.securityContext.privileged // false) == false and .securityContext.allowPrivilegeEscalation == false and ((.securityContext.capabilities.drop // []) | index("ALL")) != null and ((.securityContext.capabilities.add // []) | length) == 0] | all'
    check "1.4" "$name: no host network, PID or IPC namespace" \
        '(.spec.hostNetwork // false) == false and (.spec.hostPID // false) == false and (.spec.hostIPC // false) == false'
    check "1.5" "$name: constellation-csi runs as root (uid 0: mount(2), the root-owned hostRoot layout)" \
        '[.spec.containers[] | select(.name == "constellation-csi") | .securityContext.runAsUser == 0 and (.securityContext.runAsNonRoot // false) == false] == [true]'
    check "1.6" "$name: hostPaths exactly the plugin dir, plugins_registry, the kubelet dir, hostRoot and /dev/fuse" \
        '.kubelet as $k | .root as $r | .driver as $d | ([.spec.volumes[]? | select(.hostPath) | .hostPath.path] | sort) == (["\($k)/plugins/\($d)", "\($k)/plugins_registry", $k, $r, "/dev/fuse"] | sort)'
done
[ "$n" -gt 0 ] || { echo "[FAIL] 1.0 no node plugin pod to check"; fail=$((fail + 1)); }

# --- 2. controller --------------------------------------------------------
n=0
for name in $(jq -r '.items[] | select(.metadata.labels["app.kubernetes.io/component"] == "controller") | .metadata.name' <<<"$pods"); do
    n=$((n + 1))
    doc=$(jq --arg n "$name" '.items[] | select(.metadata.name == $n)' <<<"$pods")
    check "2.1" "$name: runs as non-root uid 65532 under RuntimeDefault seccomp" \
        '.spec.securityContext.runAsNonRoot == true and .spec.securityContext.runAsUser == 65532 and .spec.securityContext.seccompProfile.type == "RuntimeDefault"'
    check "2.2" "$name: every container unprivileged, no escalation, ALL dropped, nothing added, read-only root" \
        '[.spec.containers[] | (.securityContext.privileged // false) == false and .securityContext.allowPrivilegeEscalation == false and ((.securityContext.capabilities.drop // []) | index("ALL")) != null and ((.securityContext.capabilities.add // []) | length) == 0 and .securityContext.readOnlyRootFilesystem == true] | all'
    check "2.3" "$name: no hostPath volume, no host namespace" \
        '([.spec.volumes[]? | select(.hostPath)] | length) == 0 and (.spec.hostNetwork // false) == false and (.spec.hostPID // false) == false and (.spec.hostIPC // false) == false'
done
[ "$n" -gt 0 ] || { echo "[FAIL] 2.0 no controller pod to check"; fail=$((fail + 1)); }

# --- 3. engine pods, both owners -----------------------------------------
engine_common() { # name
    check "3.1" "$1: non-root uid 65532, RuntimeDefault seccomp" \
        '.spec.securityContext.runAsNonRoot == true and .spec.securityContext.runAsUser == 65532 and .spec.securityContext.seccompProfile.type == "RuntimeDefault"'
    check "3.2" "$1: no service-account token, the permission-less constellation-csi-engine ServiceAccount" \
        '.spec.automountServiceAccountToken == false and .spec.serviceAccountName == "constellation-csi-engine"'
    check "3.3" "$1: no host namespace, no init container, one container" \
        '(.spec.hostNetwork // false) == false and (.spec.hostPID // false) == false and (.spec.hostIPC // false) == false and (.spec.initContainers // [] | length) == 0 and (.spec.containers | length) == 1'
    check "3.4" "$1: engine container unprivileged, no escalation, ALL dropped, nothing added, read-only root, non-root" \
        '.spec.containers[0] | (.securityContext.privileged // false) == false and .securityContext.allowPrivilegeEscalation == false and ((.securityContext.capabilities.drop // []) | index("ALL")) != null and ((.securityContext.capabilities.add // []) | length) == 0 and .securityContext.readOnlyRootFilesystem == true and .securityContext.runAsNonRoot == true'
}

n=0
for name in $(jq -r '.items[] | select(.metadata.labels["app.kubernetes.io/component"] == "engine" and .metadata.labels["constellation.dev/owner"] == "controller") | .metadata.name' <<<"$pods"); do
    n=$((n + 1))
    doc=$(jq --arg n "$name" '.items[] | select(.metadata.name == $n)' <<<"$pods")
    engine_common "$name"
    check "3.5" "$name (controller-owned): emptyDir volumes only — no hostPath" \
        '[.spec.volumes[] | has("emptyDir") and (has("hostPath") | not)] | all'
done
[ "$n" -gt 0 ] || { echo "[FAIL] 3.0 no controller-owned engine pod to check"; fail=$((fail + 1)); }

n=0
for name in $(jq -r '.items[] | select(.metadata.labels["app.kubernetes.io/component"] == "engine" and .metadata.labels["constellation.dev/owner"] == "node") | .metadata.name' <<<"$pods"); do
    n=$((n + 1))
    doc=$(jq --arg n "$name" --arg root "$host_root" '.items[] | select(.metadata.name == $n) | . + {root: $root}' <<<"$pods")
    engine_common "$name"
    check "3.6" "$name (node-owned): hostPaths exactly <hostRoot>/{node-identity,sockets,policy}/<its unit>, type Directory" \
        '.metadata.labels["constellation.dev/unit"] as $u | .root as $r | ([.spec.volumes[] | select(.hostPath) | .hostPath.path] | sort) == (["\($r)/node-identity/\($u)", "\($r)/sockets/\($u)", "\($r)/policy/\($u)"] | sort) and ([.spec.volumes[] | select(.hostPath) | .hostPath.type == "Directory"] | all)'
    check "3.7" "$name (node-owned): the policy hostPath (its grant) is mounted read-only" \
        '[.spec.containers[0].volumeMounts[] | select(.name == "policy") | .readOnly == true] == [true]'
done
[ "$n" -gt 0 ] || { echo "[FAIL] 3.0 no node-owned engine pod to check"; fail=$((fail + 1)); }

# --- 4. across the namespace ---------------------------------------------
doc="$pods"
check "4.1" "the privileged containers are exactly the node plugins' constellation-csi containers" \
    '[.items[] | . as $p | (.spec.containers + (.spec.initContainers // []))[] | select(.securityContext.privileged == true) | "\($p.metadata.labels["app.kubernetes.io/component"])/\(.name)"] | unique == ["node/constellation-csi"]'
check "4.2" "no container anywhere adds a capability" \
    '[.items[] | (.spec.containers + (.spec.initContainers // []))[] | (.securityContext.capabilities.add // []) | length] | add == 0'

echo "== Summary: $pass checks PASS, $fail checks FAIL"
[ "$fail" -eq 0 ]
