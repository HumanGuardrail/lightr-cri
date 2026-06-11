#!/usr/bin/env bash
# ci/budget.sh — build-spec-r0 §7 budgets gate.
# Starts target/release/lightr-cri, measures:
#   - RSS after a crictl version + runp/rmp cycle: must be < 10240 KB (10 MB)
#   - PullImage p50 over 20 calls:                must be < 50 ms
# Requires crictl (Linux CI); exits 0 with SKIP if crictl missing (local mac).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SERVER_BIN="${LIGHTR_CRI_BIN:-${REPO_ROOT}/target/release/lightr-cri}"

RSS_LIMIT_KB=10240   # 10 MB

# ── machine-class budgets (house doctrine, hugr-lightr lineage) ──────────────
# The PRODUCT target (<50 ms, whitepaper §8) binds to the hosted-linux class
# (bare-metal/VM Linux). On the shared-docker class (Docker-on-Mac — both the
# local lane AND our self-hosted runner container) crictl wall-clock carries
# virtualization + client-spawn overhead (~50-100 ms) that is not ours; the
# class limit still catches real regressions (a resolve path that starts
# moving bytes lands in the 100s of ms). Never detect "CI" — declare the
# class explicitly via LIGHTR_BUDGET_CLASS.
BUDGET_CLASS="${LIGHTR_BUDGET_CLASS:-hosted-linux}"
case "${BUDGET_CLASS}" in
  hosted-linux)  PULL_P50_MS=50 ;;
  shared-docker) PULL_P50_MS=100 ;;
  *) echo "ERROR: unknown LIGHTR_BUDGET_CLASS '${BUDGET_CLASS}'" >&2; exit 1 ;;
esac
SKIP_P50=0
echo "budget class: ${BUDGET_CLASS} (pull p50 limit: ${PULL_P50_MS} ms; RSS: ${RSS_LIMIT_KB} KB)"

# ── probe ────────────────────────────────────────────────────────────────────
if ! command -v crictl >/dev/null 2>&1; then
  echo "SKIP budget.sh: crictl not in PATH (probe-truthful; macOS local run)" >&2
  exit 0
fi

if [ ! -x "${SERVER_BIN}" ]; then
  echo "ERROR: ${SERVER_BIN} not found; run cargo build --release first" >&2
  exit 1
fi

# ── temp workspace ───────────────────────────────────────────────────────────
TMP_DIR="$(mktemp -d)"
SOCKET="${TMP_DIR}/lightr-cri.sock"
STATE_DIR="${TMP_DIR}/state"
mkdir -p "${STATE_DIR}"

SERVER_PID=""
cleanup() {
  if [ -n "${SERVER_PID}" ] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

# ── start server ─────────────────────────────────────────────────────────────
"${SERVER_BIN}" --socket "${SOCKET}" --state "${STATE_DIR}" &
SERVER_PID=$!

for i in $(seq 1 50); do
  [ -S "${SOCKET}" ] && break
  sleep 0.1
done
if [ ! -S "${SOCKET}" ]; then
  echo "ERROR: server socket did not appear in 5s" >&2
  exit 1
fi

export CONTAINER_RUNTIME_ENDPOINT="unix://${SOCKET}"

# ── warm-up: version + runp/rmp cycle ────────────────────────────────────────
crictl version >/dev/null

# create a minimal pod sandbox config
POD_CONFIG="${TMP_DIR}/pod.json"
cat > "${POD_CONFIG}" <<'EOF'
{
  "metadata": {
    "name": "budget-pod",
    "uid": "budget-uid-1",
    "namespace": "budget-ns",
    "attempt": 0
  },
  "log_directory": "/tmp",
  "linux": {}
}
EOF

POD_ID=$(crictl runp "${POD_CONFIG}")
crictl stopp "${POD_ID}" >/dev/null
crictl rmp "${POD_ID}" >/dev/null

# ── RSS check ────────────────────────────────────────────────────────────────
RSS_KB=$(ps -o rss= -p "${SERVER_PID}" | tr -d ' ')
echo "Server RSS after warm-up: ${RSS_KB} KB (limit: ${RSS_LIMIT_KB} KB)"
if [ "${RSS_KB}" -ge "${RSS_LIMIT_KB}" ]; then
  echo "FAIL: RSS ${RSS_KB} KB >= limit ${RSS_LIMIT_KB} KB" >&2
  exit 1
fi
echo "OK: RSS within budget"

# ── pull p50 ─────────────────────────────────────────────────────────────────
if [ "${SKIP_P50}" -eq 1 ]; then
  echo "SKIP: pull p50 latency check skipped"
  echo ""
  echo "OK: all budgets passed (RSS=${RSS_KB} KB, pull_p50=SKIPPED)"
else
  TIMES_FILE="${TMP_DIR}/pull_times.txt"
  : > "${TIMES_FILE}"

  # warm-up: exclude cold-path noise (first gRPC connection, page cache)
  for i in $(seq 1 3); do
    crictl pull "ref/budget-warmup-${i}" >/dev/null 2>&1 || true
  done

  echo "Running 20 crictl pull calls for p50 measurement..."
  for i in $(seq 1 20); do
    T_START=$(date +%s%N)
    crictl pull "ref/budget-${i}" >/dev/null 2>&1 || true
    T_END=$(date +%s%N)
    # nanoseconds to milliseconds (integer)
    MS=$(( (T_END - T_START) / 1000000 ))
    echo "${MS}" >> "${TIMES_FILE}"
  done

  # compute p50: sort numerically, take element at index floor(20*0.5) = 10
  P50=$(sort -n "${TIMES_FILE}" | awk 'NR==10{print}')
  echo "Pull p50: ${P50} ms (limit: ${PULL_P50_MS} ms)"
  if [ "${P50}" -ge "${PULL_P50_MS}" ]; then
    echo "FAIL: pull p50 ${P50} ms >= limit ${PULL_P50_MS} ms" >&2
    exit 1
  fi
  echo "OK: pull p50 within budget"

  echo ""
  echo "OK: all budgets passed (RSS=${RSS_KB} KB, pull_p50=${P50} ms)"
fi
