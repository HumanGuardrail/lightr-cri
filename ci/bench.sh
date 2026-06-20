#!/usr/bin/env bash
#
# bench.sh — sign the STRUCTURAL KPIs of the daemonless / crash-only shell.
#
# Scope (HONEST):
#   IN  — properties that lightr-cri itself exercises and that this binary can
#         truthfully measure with the R1 fake backend:
#           * cold-start    : spawn → first answered CRI RPC (ready to serve)
#           * idle RSS      : resident memory of the serve process at idle
#           * crash-recovery: kill -9 → restart (same socket+state) → serving,
#                             with state re-derived (stateless / crash-only)
#           * daemonless    : process/thread count of the live runtime
#   OUT — CAS-tier KPIs (pull dedup, bytes-transferred, disk-for-N-images) are
#         properties of the REAL Lightr backend, NOT of the R1 fake backend.
#         Measuring them here would measure the fake → meaningless. They are
#         explicitly deferred to integration and reported as "out_of_scope".
#
# Reference (LABELED, not a claim): if containerd is present, its idle daemon
# RSS is captured as a reference point. Workloads differ (containerd does far
# more at idle) — this is context for the daemonless thesis, not a like-for-like
# benchmark verdict.
#
# Fail-closed: a missing CORE probe (crictl) fails under LIGHTR_CRI_REQUIRE_PROBES=1.
# The containerd reference is optional and skips loudly if absent.
#
# Output: a SIGNED JSON artifact at $BENCH_OUT (default ci/bench-results.json),
# stamped with commit, kernel, host and timestamp. No number is a "claim" until
# this file is produced by a CI run.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_BIN="${LIGHTR_CRI_BIN:-${REPO_ROOT}/target/release/lightr-cri}"
BENCH_OUT="${BENCH_OUT:-${REPO_ROOT}/ci/bench-results.json}"
REQUIRE_PROBES="${LIGHTR_CRI_REQUIRE_PROBES:-0}"

COLD_SAMPLES="${BENCH_COLD_SAMPLES:-10}"
RECOVERY_SAMPLES="${BENCH_RECOVERY_SAMPLES:-5}"

TMP_DIR="$(mktemp -d)"
SOCKET="${TMP_DIR}/cri.sock"
STATE_DIR="${TMP_DIR}/state"
SERVER_PID=""
mkdir -p "${STATE_DIR}"

cleanup() {
  if [ -n "${SERVER_PID}" ] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

die()  { echo "bench: FATAL: $*" >&2; exit 1; }
note() { echo "bench: $*" >&2; }

[ -x "${SERVER_BIN}" ] || die "server binary not found/executable: ${SERVER_BIN} (build release first)"

if ! command -v crictl >/dev/null 2>&1; then
  if [ "${REQUIRE_PROBES}" = "1" ]; then
    die "crictl not in PATH and LIGHTR_CRI_REQUIRE_PROBES=1 (cannot sign bench)"
  fi
  note "SKIP: crictl not in PATH (probe-truthful); no bench signed"
  exit 0
fi
export CONTAINER_RUNTIME_ENDPOINT="unix://${SOCKET}"
export IMAGE_SERVICE_ENDPOINT="unix://${SOCKET}"

now_ns() { date +%s%N; }

# Spawn the server in the CURRENT shell (NOT a command-substitution subshell —
# that would not propagate SERVER_PID back to the parent, leaking the process and
# colliding on the socket). Returns 0 once the server answers its first CRI RPC,
# and writes the spawn→first-RPC latency (ms) into the global LAST_MS. Returns 1
# (not a hard die) on startup failure so the caller can decide.
LAST_MS=""
start_server_timed() {
  local t0 t1
  t0="$(now_ns)"
  "${SERVER_BIN}" --socket "${SOCKET}" --state "${STATE_DIR}" >/dev/null 2>&1 &
  SERVER_PID=$!
  # Poll for the first ANSWERED RPC (proves it actually serves, not just bound).
  for _ in $(seq 1 600); do
    if crictl version >/dev/null 2>&1; then
      t1="$(now_ns)"
      LAST_MS=$(( (t1 - t0) / 1000000 ))
      return 0
    fi
    kill -0 "${SERVER_PID}" 2>/dev/null || { note "server died during startup"; return 1; }
    sleep 0.05
  done
  note "server did not answer an RPC within 30s"
  return 1
}

stop_server() {
  if [ -n "${SERVER_PID}" ] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  SERVER_PID=""
  rm -f "${SOCKET}"
}

# Summary stats (min / p50 nearest-rank / max) over a newline list of integers.
stats() {
  sort -n | awk '
    { a[NR]=$1 }
    END {
      n=NR
      if (n==0) { print "null,null,null"; exit }
      p=int((n+1)/2); if (p<1) p=1
      printf "%d,%d,%d", a[1], a[p], a[n]
    }'
}

# ── 1. COLD-START: spawn → first answered RPC ────────────────────────────────
note "cold-start × ${COLD_SAMPLES} …"
cold_file="${TMP_DIR}/cold.txt"; : > "${cold_file}"
for _ in $(seq 1 "${COLD_SAMPLES}"); do
  start_server_timed || die "cold-start failed (server never answered)"
  echo "${LAST_MS}" >> "${cold_file}"
  stop_server
  rm -rf "${STATE_DIR}"; mkdir -p "${STATE_DIR}"   # fresh state each cold run
done
IFS=',' read -r cold_min cold_p50 cold_max < <(stats < "${cold_file}")
note "cold-start ms: min=${cold_min} p50=${cold_p50} max=${cold_max}"

# ── 2. IDLE RSS + daemonless footprint (warm server) ─────────────────────────
note "idle footprint …"
start_server_timed || die "idle-footprint: server never answered"
# Warm up: one pod lifecycle so RSS reflects a real working set, then settle.
POD_JSON="${TMP_DIR}/pod.json"
cat > "${POD_JSON}" <<'JSON'
{ "metadata": { "name": "bench-pod", "namespace": "bench", "uid": "bench-uid-0", "attempt": 0 },
  "log_directory": "/tmp", "linux": {} }
JSON
POD_ID="$(crictl runp "${POD_JSON}" 2>/dev/null || true)"
sleep 0.3
RSS_KB="$(ps -o rss= -p "${SERVER_PID}" | tr -d ' ')"
THREADS="$(ps -o nlwp= -p "${SERVER_PID}" 2>/dev/null | tr -d ' ' || echo null)"
# Daemonless proof: how many long-lived runtime processes exist. lightr-cri is
# the single serve process — there is no separate daemon tree.
PROC_COUNT="$( { pgrep -f "$(basename "${SERVER_BIN}") --socket ${SOCKET}" 2>/dev/null || true; } | wc -l | tr -d ' ')"
note "idle RSS=${RSS_KB} KB  threads=${THREADS}  runtime_procs=${PROC_COUNT}"

# ── 3. CRASH-RECOVERY: kill -9 → restart → serving, state re-derived ─────────
note "crash-recovery × ${RECOVERY_SAMPLES} …"
rec_file="${TMP_DIR}/rec.txt"; : > "${rec_file}"
# Only claim re-derivation if there is actually a pod to re-derive; otherwise
# report "untested" rather than a hollow "true".
if [ -n "${POD_ID:-}" ]; then rederive_ok=true; else rederive_ok=untested; fi
for _ in $(seq 1 "${RECOVERY_SAMPLES}"); do
  # Hard-kill the live server (crash-only law: no graceful shutdown).
  kill -9 "${SERVER_PID}" 2>/dev/null || true
  wait "${SERVER_PID}" 2>/dev/null || true
  SERVER_PID=""
  rm -f "${SOCKET}"
  # Restart on the SAME socket+state and time to first answered RPC.
  start_server_timed || die "crash-recovery: server did not come back"
  echo "${LAST_MS}" >> "${rec_file}"
  # Re-derivation proof: the pod created before the crash must still list,
  # rebuilt from disk/kernel — nothing was kept in the dead process.
  if [ -n "${POD_ID:-}" ] && ! crictl pods -q 2>/dev/null | grep -q "${POD_ID:0:12}"; then
    rederive_ok=false
  fi
done
IFS=',' read -r rec_min rec_p50 rec_max < <(stats < "${rec_file}")
note "crash-recovery ms: min=${rec_min} p50=${rec_p50} max=${rec_max}  state_rederived=${rederive_ok}"
stop_server

# ── 4. REFERENCE (labeled): containerd idle daemon RSS ───────────────────────
ctd_rss="null"; ctd_note="containerd not present — reference skipped"
if command -v containerd >/dev/null 2>&1; then
  CTD_ROOT="${TMP_DIR}/ctd-root"; CTD_STATE="${TMP_DIR}/ctd-state"
  CTD_SOCK="${TMP_DIR}/ctd.sock"
  mkdir -p "${CTD_ROOT}" "${CTD_STATE}"
  containerd --root "${CTD_ROOT}" --state "${CTD_STATE}" \
    --address "${CTD_SOCK}" >/dev/null 2>&1 &
  CTD_PID=$!
  for _ in $(seq 1 100); do [ -S "${CTD_SOCK}" ] && break; sleep 0.05; done
  sleep 0.5
  if kill -0 "${CTD_PID}" 2>/dev/null; then
    ctd_rss="$(ps -o rss= -p "${CTD_PID}" | tr -d ' ')"
    ctd_note="idle containerd daemon RSS — REFERENCE ONLY; workloads differ (containerd does far more at idle)"
  fi
  kill "${CTD_PID}" 2>/dev/null || true; wait "${CTD_PID}" 2>/dev/null || true
  note "reference: containerd idle RSS=${ctd_rss} KB"
else
  note "reference: ${ctd_note}"
fi

# ── 5. SIGN: emit the JSON artifact ──────────────────────────────────────────
GIT_SHA="${GITHUB_SHA:-$(git -C "${REPO_ROOT}" rev-parse HEAD 2>/dev/null || echo unknown)}"
KERNEL="$(uname -sr)"
HOSTLBL="${RUNNER_NAME:-$(uname -n)}"
STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

j() { [ -z "$1" ] && echo null || echo "$1"; }

cat > "${BENCH_OUT}" <<JSON
{
  "schema": "lightr-cri.bench/v1",
  "signed": {
    "commit": "${GIT_SHA}",
    "kernel": "${KERNEL}",
    "host": "${HOSTLBL}",
    "timestamp_utc": "${STAMP}",
    "backend": "fake (R1)"
  },
  "in_scope": {
    "cold_start_ms":     { "n": ${COLD_SAMPLES},     "min": $(j "${cold_min}"), "p50": $(j "${cold_p50}"), "max": $(j "${cold_max}"), "definition": "spawn -> first answered CRI RPC" },
    "idle_rss_kb":       { "value": $(j "${RSS_KB}"), "threads": $(j "${THREADS}"), "runtime_processes": $(j "${PROC_COUNT}"), "definition": "VmRSS of the serve process after one pod lifecycle" },
    "crash_recovery_ms": { "n": ${RECOVERY_SAMPLES}, "min": $(j "${rec_min}"), "p50": $(j "${rec_p50}"), "max": $(j "${rec_max}"), "state_rederived": "${rederive_ok}", "definition": "kill -9 -> restart same socket+state -> first answered RPC" }
  },
  "reference": {
    "containerd_idle_rss_kb": $(j "${ctd_rss}"),
    "note": "${ctd_note}"
  },
  "out_of_scope": {
    "note": "CAS-tier KPIs (pull dedup, bytes-transferred, disk-for-N-images) are properties of the REAL Lightr backend, not the R1 fake backend. Deferred to integration; not measurable here without measuring the fake.",
    "deferred_kpis": ["pull_dedup_ratio", "pull_bytes_transferred", "disk_for_n_similar_images"]
  }
}
JSON

# Self-validate the signed artifact: a malformed JSON must FAIL the step, not
# pass with a hollow file (jq is present in CI — used by the critest gate).
if command -v jq >/dev/null 2>&1; then
  jq -e . "${BENCH_OUT}" >/dev/null || die "emitted bench JSON is malformed: ${BENCH_OUT}"
elif [ "${REQUIRE_PROBES}" = "1" ]; then
  die "jq not found and LIGHTR_CRI_REQUIRE_PROBES=1 (cannot validate signed JSON)"
fi

note "signed → ${BENCH_OUT}"
echo "──────── lightr-cri bench (signed) ────────"
cat "${BENCH_OUT}"
echo "───────────────────────────────────────────"
