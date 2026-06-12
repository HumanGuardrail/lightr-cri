#!/usr/bin/env sh
# STUB: mock CNI plugin for unprivileged vector testing.
#
# This is NOT a real CNI plugin. It ignores stdin and all CNI environment
# variables (CNI_COMMAND, CNI_NETNS, CNI_CONTAINERID, etc.) and simply echoes
# a canned CNI result JSON to stdout, then exits 0.
#
# Purpose: allow vectors/bin/ to stand in as the CNI_PATH for tests that
# exercise the CNI invocation path without requiring CAP_NET_ADMIN, a real
# network namespace, or actual CNI plugin binaries. Privileged real-CNI
# vectors live in crates/lightr-cri-acceptance/ (B-items, REQUIRE_PROBES=1).
#
# Canned result: one IP address on the 10.88.0.0/16 bridge network (matches
# the default podman/CNI bridge; chosen arbitrarily for test plausibility).

cat <<'EOF'
{"cniVersion":"1.0.0","ips":[{"address":"10.88.0.42/16"}]}
EOF
