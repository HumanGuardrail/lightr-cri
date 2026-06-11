#!/usr/bin/env bash
# ci/greenlist-gate.sh — build-spec-r0 §7 R0 exit law.
# Rules:
#   1. tests/GREENLIST must exist (always).
#   2. On CI main branch (GITHUB_REF=refs/heads/main): GREENLIST must be
#      non-empty (at least one conquered critest item).
#      On PRs / other branches: empty GREENLIST is allowed (first conquest
#      has not happened yet).
# '#' comments and blank lines are ignored when checking emptiness.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GREENLIST="${REPO_ROOT}/tests/GREENLIST"

# ── rule 1: GREENLIST must exist ─────────────────────────────────────────────
if [ ! -f "${GREENLIST}" ]; then
  echo "FAIL: tests/GREENLIST does not exist" >&2
  exit 1
fi
echo "OK: tests/GREENLIST exists"

# ── rule 2: non-empty on main branch ─────────────────────────────────────────
GITHUB_REF="${GITHUB_REF:-}"

if [ "${GITHUB_REF}" = "refs/heads/main" ]; then
  # count non-blank, non-comment lines
  ITEM_COUNT=$(grep -cv '^\s*\(#\|$\)' "${GREENLIST}" || true)
  if [ "${ITEM_COUNT}" -eq 0 ]; then
    echo "FAIL: tests/GREENLIST is empty on main branch (R0 exit law: at least one conquered item required)" >&2
    exit 1
  fi
  echo "OK: GREENLIST has ${ITEM_COUNT} conquered item(s) on main"
else
  ITEM_COUNT=$(grep -cv '^\s*\(#\|$\)' "${GREENLIST}" || true)
  echo "OK: GREENLIST check (non-main branch): ${ITEM_COUNT} conquered item(s) (empty allowed on PRs)"
fi
