#!/usr/bin/env bash
# ci/kubelet-smoke.sh — B7 kubelet smoke gate (R1 exit criterion).
#
# Starts lightr-cri on a well-known socket, writes a static-pod hostNetwork
# manifest, starts kubelet v1.33.13 (standalone, no apiserver), polls
# readOnlyPort 10255/pods for phase Running within 120s.
#
# Requires: privileged-ish runner (SYS_ADMIN+NET_ADMIN / --privileged),
#   /dev/kmsg (cadvisor OOM watcher; present in --privileged containers),
#   /sys/fs/cgroup writable.
# If running outside such an environment, this script loudly SKIPs (probe-
# truthful) instead of producing a false-green.
#
# Environment:
#   LIGHTR_CRI_BIN   path to lightr-cri binary (default: target/release/lightr-cri)
#   KUBELET_BIN      path to kubelet binary (default: /tmp/kubelet-cache/kubelet)
set -euo pipefail

# ── probe: are we in a privileged-enough container? ──────────────────────────
probe_privileged() {
  # Require /dev/kmsg (cadvisor) and writable cgroup mount.
  if [ ! -e /dev/kmsg ]; then
    echo "SKIP: /dev/kmsg absent — not a privileged container (cadvisor fatal)" >&2
    exit 0
  fi
  # Check for SYS_ADMIN by attempting a no-op unshare; ignore if unshare absent.
  if command -v unshare >/dev/null 2>&1; then
    if ! unshare -n true 2>/dev/null; then
      echo "SKIP: unshare -n failed — CAP_SYS_ADMIN absent (needed for kubelet cgroup ops)" >&2
      exit 0
    fi
  fi
}
probe_privileged

# ── paths ─────────────────────────────────────────────────────────────────────
LIGHTR_CRI_BIN="${LIGHTR_CRI_BIN:-$(dirname "$0")/../target/release/lightr-cri}"
KUBELET_BIN="${KUBELET_BIN:-/tmp/kubelet-cache/kubelet}"
SOCK="/run/lightr/lightr.sock"
KUBELET_CONF="/tmp/kubelet-smoke-config.yaml"
MANIFESTS_DIR="/etc/kubernetes/manifests"
SMOKE_POD_MANIFEST="${MANIFESTS_DIR}/smoke.yaml"
LOG_DIR="/tmp/kubelet-smoke-logs"

mkdir -p "${LOG_DIR}"
mkdir -p "$(dirname "${SOCK}")"
mkdir -p "${MANIFESTS_DIR}"

# ── cleanup trap ──────────────────────────────────────────────────────────────
KUBELET_PID=""
SERVER_PID=""
cleanup() {
  echo "--- kubelet-smoke: cleanup ---"
  if [ -n "${KUBELET_PID}" ] && kill -0 "${KUBELET_PID}" 2>/dev/null; then
    kill "${KUBELET_PID}" 2>/dev/null || true
    wait "${KUBELET_PID}" 2>/dev/null || true
  fi
  if [ -n "${SERVER_PID}" ] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -f "${SOCK}" "${KUBELET_CONF}" "${SMOKE_POD_MANIFEST}" || true
}
trap cleanup EXIT

# ── verify kubelet binary present ─────────────────────────────────────────────
if [ ! -x "${KUBELET_BIN}" ]; then
  echo "FAIL: kubelet binary not found at ${KUBELET_BIN}" >&2
  echo "  Run the 'install CNI plugins and kubelet' step first." >&2
  exit 1
fi

# ── verify lightr-cri binary present ──────────────────────────────────────────
if [ ! -x "${LIGHTR_CRI_BIN}" ]; then
  echo "FAIL: lightr-cri binary not found at ${LIGHTR_CRI_BIN}" >&2
  exit 1
fi

# ── 1. start lightr-cri server ────────────────────────────────────────────────
echo "--- kubelet-smoke: starting lightr-cri on ${SOCK} ---"
LIGHTR_CRI_REQUIRE_PROBES=1 "${LIGHTR_CRI_BIN}" \
  --socket "${SOCK}" \
  >"${LOG_DIR}/lightr-cri.log" 2>&1 &
SERVER_PID=$!

# wait for socket to appear (up to 10s)
SOCK_WAIT=0
until [ -S "${SOCK}" ] || [ ${SOCK_WAIT} -ge 10 ]; do
  sleep 1
  SOCK_WAIT=$((SOCK_WAIT + 1))
done
if [ ! -S "${SOCK}" ]; then
  echo "FAIL: lightr-cri socket did not appear within 10s" >&2
  echo "--- lightr-cri log ---" >&2
  cat "${LOG_DIR}/lightr-cri.log" >&2
  exit 1
fi
echo "  socket up: ${SOCK}"

# ── 2. write KubeletConfiguration (verbatim from r1-kubelet-smoke.md) ─────────
echo "--- kubelet-smoke: writing KubeletConfiguration ---"
cat >"${KUBELET_CONF}" <<'KUBELET_EOF'
apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
containerRuntimeEndpoint: unix:///run/lightr/lightr.sock
staticPodPath: /etc/kubernetes/manifests
enableServer: false
readOnlyPort: 10255
address: 127.0.0.1
authentication:
  webhook:
    enabled: false
authorization:
  mode: AlwaysAllow
failSwapOn: false
cgroupDriver: cgroupfs
cgroupsPerQOS: false
enforceNodeAllocatable: ["none"]
localStorageCapacityIsolation: false
makeIPTablesUtilChains: false
fileCheckFrequency: 5s
logging:
  format: text
KUBELET_EOF

# ── 3. write static smoke pod (hostNetwork; sleep infinity; IfNotPresent) ─────
echo "--- kubelet-smoke: writing static smoke pod manifest ---"
cat >"${SMOKE_POD_MANIFEST}" <<'POD_EOF'
apiVersion: v1
kind: Pod
metadata:
  name: smoke
  namespace: default
spec:
  hostNetwork: true
  restartPolicy: Always
  containers:
    - name: smoke
      image: ref/smoke
      imagePullPolicy: IfNotPresent
      command: ["sleep", "infinity"]
POD_EOF

# ── 4. start kubelet ──────────────────────────────────────────────────────────
echo "--- kubelet-smoke: starting kubelet v1.33.13 (standalone) ---"
"${KUBELET_BIN}" \
  --config="${KUBELET_CONF}" \
  --hostname-override=smoke-node \
  --v=4 \
  >"${LOG_DIR}/kubelet.log" 2>&1 &
KUBELET_PID=$!
echo "  kubelet pid: ${KUBELET_PID}"

# ── 5. poll readOnlyPort for smoke pod phase Running (120s timeout) ───────────
echo "--- kubelet-smoke: polling 127.0.0.1:10255/pods (timeout 120s) ---"
DEADLINE=$(($(date +%s) + 120))
PHASE=""
while [ "$(date +%s)" -lt "${DEADLINE}" ]; do
  # If kubelet died, fail immediately.
  if ! kill -0 "${KUBELET_PID}" 2>/dev/null; then
    echo "FAIL: kubelet process exited prematurely" >&2
    echo "--- kubelet log (tail 40) ---" >&2
    tail -40 "${LOG_DIR}/kubelet.log" >&2
    exit 1
  fi
  # Poll /pods endpoint; tolerate connection refused during startup.
  PODS_JSON=$(curl -sf --max-time 5 http://127.0.0.1:10255/pods 2>/dev/null || true)
  if [ -n "${PODS_JSON}" ]; then
    # Extract the phase of the smoke pod (name contains "smoke").
    PHASE=$(python3 -c "
import sys, json
data = json.loads('''${PODS_JSON}'''.replace(\"'''\", ''))
items = data.get('items', [])
for pod in items:
    name = pod.get('metadata', {}).get('name', '')
    if 'smoke' in name:
        phase = pod.get('status', {}).get('phase', '')
        print(phase)
        sys.exit(0)
print('')
" 2>/dev/null || true)
    if [ "${PHASE}" = "Running" ]; then
      echo "  smoke pod phase: Running — SUCCESS"
      break
    fi
    echo "  smoke pod phase: '${PHASE}' — waiting..."
  fi
  sleep 2
done

if [ "${PHASE}" != "Running" ]; then
  echo "FAIL: smoke pod did not reach Running phase within 120s (last phase: '${PHASE}')" >&2
  echo "--- kubelet log (tail 60) ---" >&2
  tail -60 "${LOG_DIR}/kubelet.log" >&2
  echo "--- lightr-cri log (tail 40) ---" >&2
  tail -40 "${LOG_DIR}/lightr-cri.log" >&2
  exit 1
fi

# ── 6. assert kubelet log has no CRI-fatal lines ──────────────────────────────
# Allowlist: these are tolerated UNIMPLEMENTED/error-logged RPCs per the
# r1-kubelet-smoke.md matrix (RuntimeConfig, ImageFsInfo, ListImages,
# ListContainerStats, UpdateRuntimeConfig, ReopenContainerLog, PodSandboxStats,
# ListPodSandboxStats, GetContainerEvents, ListMetricDescriptors,
# ListPodSandboxMetrics — all tolerated per kubelet source analysis).
echo "--- kubelet-smoke: checking kubelet log for CRI-fatal lines ---"
TOLERATED_RPCS="RuntimeConfig|ImageFsInfo|ListImages|ListContainerStats\
|UpdateRuntimeConfig|ReopenContainerLog|PodSandboxStats|ListPodSandboxStats\
|GetContainerEvents|ListMetricDescriptors|ListPodSandboxMetrics\
|[Uu]nimplemented"

FATAL_LINES=$(grep -iE 'cri.*fatal|fatal.*cri|FATAL' "${LOG_DIR}/kubelet.log" \
  | grep -vE "${TOLERATED_RPCS}" \
  || true)

if [ -n "${FATAL_LINES}" ]; then
  echo "FAIL: CRI-fatal lines found in kubelet log (not in tolerated allowlist):" >&2
  echo "${FATAL_LINES}" >&2
  exit 1
fi
echo "  no CRI-fatal lines (non-tolerated) — PASS"

echo ""
echo "=== kubelet-smoke: PASS ==="
echo "  smoke pod reached Running within 120s; kubelet log clean."
