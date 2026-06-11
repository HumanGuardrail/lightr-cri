#!/usr/bin/env bash
# ci/critest-gate.sh — A10: run critest with frozen skip-list; enforce GREENLIST.
# Starts target/release/lightr-cri on a tmp socket+state, runs critest, then:
#   - Every item in tests/GREENLIST must have passed  → exit 1 on regression.
#   - Newly passing items are printed as add candidates (NOT auto-written).
# Server is killed on exit via trap.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SKIPS_FILE="${REPO_ROOT}/ci/critest-skips.txt"
GREENLIST="${REPO_ROOT}/tests/GREENLIST"
SERVER_BIN="${REPO_ROOT}/target/release/lightr-cri"

# ── pre-flight ───────────────────────────────────────────────────────────────
if ! command -v critest >/dev/null 2>&1; then
  echo "SKIP critest-gate: critest not in PATH (probe-truthful; see build-spec-r0 §7)" >&2
  exit 0
fi

if [ ! -x "${SERVER_BIN}" ]; then
  echo "ERROR: ${SERVER_BIN} not found; run cargo build --release first" >&2
  exit 1
fi

if [ ! -f "${SKIPS_FILE}" ]; then
  echo "ERROR: ${SKIPS_FILE} missing" >&2
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

# wait up to 5 s for socket to appear
for _w in $(seq 1 50); do
  [ -S "${SOCKET}" ] && break
  sleep 0.1
done
if [ ! -S "${SOCKET}" ]; then
  echo "ERROR: server socket did not appear in 5s" >&2
  exit 1
fi

# ── build ginkgo skip regex from skip-list ───────────────────────────────────
# Strip comment lines and blank lines; join non-comment parts by |.
# Each line: <regex>  # reason → ...   — take the part before the first '#'.
SKIP_REGEX=$(grep -v '^\s*#' "${SKIPS_FILE}" | grep -v '^\s*$' \
  | sed 's/#.*//' | sed 's/[[:space:]]*$//' | grep -v '^$' \
  | paste -sd '|' -)

# ── run critest ──────────────────────────────────────────────────────────────
CRITEST_OUT="${TMP_DIR}/critest.out"
echo "Running critest (skip: ${SKIP_REGEX:-<none>})"
set +e
critest \
  --runtime-endpoint "unix://${SOCKET}" \
  --ginkgo.skip "${SKIP_REGEX}" \
  --ginkgo.v \
  2>&1 | tee "${CRITEST_OUT}"
CRITEST_EXIT=$?
set -e

# ── parse passed items ───────────────────────────────────────────────────────
# critest/ginkgo marks passed items with "[It]" and "PASSED" or the final
# "[PASS]" summary per spec.  We extract the spec names from "passed" lines.
# Ginkgo v2 format: lines like "  [It] <spec name>" followed by "[PASSED]"
# or in summary: "• [PASS] <spec name>".
# We capture both formats conservatively.
mapfile -t PASSED_ITEMS < <(
  grep -E '^\s+\[It\]|\[PASS\]|--- PASS:' "${CRITEST_OUT}" \
    | sed -E 's/^\s+\[It\] //' \
          -e 's/^.*\[PASS\] //' \
          -e 's/^--- PASS: //' \
          -e 's/ \(.*$//' \
    | sed 's/^[[:space:]]*//' | grep -v '^$' | sort -u
)

# ── GREENLIST enforcement ────────────────────────────────────────────────────
REGRESSION=0
if [ -f "${GREENLIST}" ]; then
  while IFS= read -r line; do
    # skip blank and comment lines
    [[ "${line}" =~ ^[[:space:]]*$ ]] && continue
    [[ "${line}" =~ ^[[:space:]]*# ]] && continue
    ITEM="${line}"
    # check item appears in the passed list
    found=0
    for p in "${PASSED_ITEMS[@]+"${PASSED_ITEMS[@]}"}"; do
      if [ "${p}" = "${ITEM}" ]; then
        found=1
        break
      fi
    done
    if [ "${found}" -eq 0 ]; then
      echo "REGRESSION: GREENLIST item not in critest passed list: ${ITEM}" >&2
      REGRESSION=1
    fi
  done < "${GREENLIST}"
fi

# ── newly passing candidates (informational only — do NOT auto-write) ────────
if [ -f "${GREENLIST}" ]; then
  echo ""
  echo "── newly passing items (add candidates for GREENLIST — NOT auto-written) ──"
  NEWLY_PASSING=0
  for p in "${PASSED_ITEMS[@]+"${PASSED_ITEMS[@]}"}"; do
    [ -z "${p}" ] && continue
    in_greenlist=0
    while IFS= read -r gl; do
      [[ "${gl}" =~ ^[[:space:]]*$ ]] && continue
      [[ "${gl}" =~ ^[[:space:]]*# ]] && continue
      if [ "${gl}" = "${p}" ]; then
        in_greenlist=1
        break
      fi
    done < "${GREENLIST}"
    if [ "${in_greenlist}" -eq 0 ]; then
      echo "CANDIDATE: ${p}"
      NEWLY_PASSING=1
    fi
  done
  [ "${NEWLY_PASSING}" -eq 0 ] && echo "(none)"
fi

# ── final result ─────────────────────────────────────────────────────────────
if [ "${REGRESSION}" -ne 0 ]; then
  echo "" >&2
  echo "FAIL: critest-gate: GREENLIST regression detected (see above)" >&2
  exit 1
fi

# Fail-closed law (cold-critic finding 2026-06-11): a broken runtime or a
# broken parser must be RED, never a warning. Honest red over green lie.
if [ "${#PASSED_ITEMS[@]}" -eq 0 ]; then
  echo "" >&2
  echo "FAIL: critest-gate: zero passed items extracted — either the runtime" >&2
  echo "      failed everything or the ginkgo output parser no longer matches" >&2
  echo "      critest v1.33.0 format. Record a real output sample and fix." >&2
  exit 1
fi

if [ "${CRITEST_EXIT}" -ne 0 ]; then
  echo "" >&2
  echo "FAIL: critest exited ${CRITEST_EXIT}. Non-skipped items failed." >&2
  echo "      Either conquer them (add to GREENLIST when green) or move them" >&2
  echo "      to ci/critest-skips.txt with an explicit '# reason → R1' line." >&2
  exit 1
fi

echo "OK: critest-gate passed (critest green on non-skipped set, no GREENLIST regressions)"
