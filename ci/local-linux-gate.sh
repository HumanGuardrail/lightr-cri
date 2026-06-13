#!/usr/bin/env bash
# ci/local-linux-gate.sh — run the FULL Linux conformance lane locally via
# Docker (mirror of .github/workflows/ci.yml linux-conformance), for machines
# without GitHub Actions minutes. Same pinned cri-tools, same sha256s, same
# fail-closed gates. Artifacts go to target-linux/ (gitignored).
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

exec docker run --rm \
  --cap-add SYS_ADMIN --cap-add NET_ADMIN --security-opt apparmor=unconfined \
  -v "${REPO_ROOT}:/work" -w /work \
  -e CARGO_TARGET_DIR=/work/target-linux \
  -e LIGHTR_CRI_BIN=/work/target-linux/release/lightr-cri \
  -e KUBELET_BIN=/tmp/kubelet-cache/kubelet \
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
    apt-get update -qq && apt-get install -y -qq jq python3 iptables iproute2 >/dev/null
    tar -C /usr/local/bin -xzf /tmp/crictl.tgz
    tar -C /usr/local/bin -xzf /tmp/critest.tgz
    crictl --version && critest --version || true

    # ── CNI plugins v1.9.1 (pinned sha256) ─────────────────────────────────
    CNI_SHA256=b98f74a0f8522f0a83867178729c1aa70f2158f90c45a2ca8fa791db1c76b303
    CNI_URL=https://github.com/containernetworking/plugins/releases/download/v1.9.1/cni-plugins-linux-amd64-v1.9.1.tgz
    echo "--- install: CNI plugins v1.9.1 ---"
    curl -fsSL -o /tmp/cni-plugins.tgz "${CNI_URL}"
    echo "${CNI_SHA256}  /tmp/cni-plugins.tgz" | sha256sum -c -
    mkdir -p /opt/cni/bin
    tar -C /opt/cni/bin -xzf /tmp/cni-plugins.tgz
    rm /tmp/cni-plugins.tgz
    # Write bridge+host-local+portmap conflist (smallest honest setup per r1-cni.md)
    mkdir -p /etc/cni/net.d
    cat >/etc/cni/net.d/10-lightr.conflist <<'"'"'CONFLIST_EOF'"'"'
{
  "cniVersion": "1.0.0",
  "name": "lightr",
  "plugins": [
    {
      "type": "bridge",
      "bridge": "cni0",
      "isGateway": true,
      "ipMasq": true,
      "ipam": {
        "type": "host-local",
        "subnet": "10.88.0.0/16",
        "routes": [{ "dst": "0.0.0.0/0" }]
      }
    },
    {
      "type": "portmap",
      "capabilities": { "portMappings": true }
    }
  ]
}
CONFLIST_EOF
    echo "  CNI plugins installed to /opt/cni/bin; conflist written."

    # ── kubelet v1.33.13 (pinned sha256) ────────────────────────────────────
    KUBELET_SHA256=0415ef7778646172f7aad830cbe09a7d903461eef9e15cd34193b85d95dfff0d
    KUBELET_URL=https://dl.k8s.io/release/v1.33.13/bin/linux/amd64/kubelet
    KUBELET_CACHE=/tmp/kubelet-cache
    echo "--- install: kubelet v1.33.13 ---"
    mkdir -p "${KUBELET_CACHE}"
    curl -fsSL -o "${KUBELET_CACHE}/kubelet" "${KUBELET_URL}"
    echo "${KUBELET_SHA256}  ${KUBELET_CACHE}/kubelet" | sha256sum -c -
    chmod +x "${KUBELET_CACHE}/kubelet"
    echo "  kubelet cached at ${KUBELET_CACHE}/kubelet"

    rustup component add clippy 2>/dev/null || true
    cargo build --release --workspace
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace
    bash ci/critest-gate.sh
    bash ci/budget.sh
    bash ci/greenlist-gate.sh

    # ── B7 kubelet-smoke gate ────────────────────────────────────────────────
    echo "--- B7 kubelet-smoke gate ---"
    bash ci/kubelet-smoke.sh

    echo "LOCAL-LINUX-GATE: ALL GREEN"
  '
