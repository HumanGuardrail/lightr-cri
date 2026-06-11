#!/usr/bin/env bash
# ci/runners/linux-runner.sh — provision the SELF-HOSTED Linux runner as a
# Docker container on a macOS/amd64 host. Idempotent: --replace re-registers.
#
# Usage: TOKEN=<registration token> bash ci/runners/linux-runner.sh
#   token: gh api -X POST repos/humangr-labs/lightr-cri/actions/runners/registration-token --jq .token
#
# The container ships everything the ci.yml jobs assume: rust 1.96 (+clippy
# via the workflow step), sudo, unzip (protoc install step), libicu (runner).
set -euo pipefail
: "${TOKEN:?set TOKEN to a runner registration token}"

RUNNER_VER="2.335.1"
NAME="lightr-cri-runner-linux"

docker rm -f "${NAME}" 2>/dev/null || true
docker run -d --name "${NAME}" --restart unless-stopped \
  -e RUNNER_ALLOW_RUNASROOT=1 -e TOKEN="${TOKEN}" -e RUNNER_VER="${RUNNER_VER}" \
  rust:1.96-bookworm bash -c '
    set -e
    apt-get update -qq && apt-get install -y -qq libicu72 curl sudo unzip jq >/dev/null
    mkdir -p /runner && cd /runner
    curl -fsSL -o runner.tgz "https://github.com/actions/runner/releases/download/v${RUNNER_VER}/actions-runner-linux-x64-${RUNNER_VER}.tar.gz"
    tar xzf runner.tgz && rm runner.tgz
    ./config.sh --unattended --url https://github.com/humangr-labs/lightr-cri \
      --token "$TOKEN" --name lightr-linux-docker --labels self-hosted,linux,x64,lightr \
      --work _work --replace
    exec ./run.sh
  '
echo "runner container started: ${NAME} (watch: docker logs -f ${NAME})"
