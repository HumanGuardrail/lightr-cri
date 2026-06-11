#!/usr/bin/env bash
# ci/local-linux-gate.sh — run the FULL Linux conformance lane locally via
# Docker (mirror of .github/workflows/ci.yml linux-conformance), for machines
# without GitHub Actions minutes. Same pinned cri-tools, same sha256s, same
# fail-closed gates. Artifacts go to target-linux/ (gitignored).
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

exec docker run --rm \
  -v "${REPO_ROOT}:/work" -w /work \
  -e CARGO_TARGET_DIR=/work/target-linux \
  -e LIGHTR_CRI_BIN=/work/target-linux/release/lightr-cri \
  -e LIGHTR_CRI_REQUIRE_PROBES=1 \
  -e LIGHTR_BUDGET_CLASS=shared-docker \
  rust:1.96-bookworm bash -ec '
    set -euo pipefail
    CRICTL_SHA=8307399e714626e69d1213a4cd18c8dec3d0201ecdac009b1802115df8973f0f
    CRITEST_SHA=31baec20eda89276b3466ec24cfdfc54258baf27651997db7b5d4205f60ea61b
    BASE=https://github.com/kubernetes-sigs/cri-tools/releases/download/v1.33.0
    curl -fsSL -o /tmp/crictl.tgz  "${BASE}/crictl-v1.33.0-linux-amd64.tar.gz"
    echo "${CRICTL_SHA}  /tmp/crictl.tgz"  | sha256sum -c -
    curl -fsSL -o /tmp/critest.tgz "${BASE}/critest-v1.33.0-linux-amd64.tar.gz"
    echo "${CRITEST_SHA}  /tmp/critest.tgz" | sha256sum -c -
    apt-get update -qq && apt-get install -y -qq jq >/dev/null
    tar -C /usr/local/bin -xzf /tmp/crictl.tgz
    tar -C /usr/local/bin -xzf /tmp/critest.tgz
    crictl --version && critest --version || true

    rustup component add clippy 2>/dev/null || true
    cargo build --release --workspace
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace
    bash ci/critest-gate.sh
    bash ci/budget.sh
    bash ci/greenlist-gate.sh
    echo "LOCAL-LINUX-GATE: ALL GREEN"
  '
