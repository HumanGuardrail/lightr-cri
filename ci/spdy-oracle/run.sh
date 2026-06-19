#!/usr/bin/env bash
# Build the Rust SPDY harness + the client-go SPDY oracle, run the harness,
# point the oracle at its URL, and report whether client-go can open the
# stream (the critest "failed to open streamer" repro, isolated).
set -euo pipefail

export CARGO_TARGET_DIR=/work/target-linux
export PATH="/usr/local/go/bin:$PATH"

echo "=== building rust harness ==="
cargo build --release -p lightr-cri-stream --example spdy_harness

echo "=== building go oracle ==="
cd /work/ci/spdy-oracle
go mod tidy >/dev/null 2>&1 || true
go build -o /tmp/oracle .
cd /work

echo "=== starting harness ==="
LIGHTR_SPDY_TRACE=1 /work/target-linux/release/examples/spdy_harness > /tmp/harness.out 2>/tmp/harness.err &
HPID=$!
# wait for the URL+token line
for i in $(seq 1 50); do
  if [ -s /tmp/harness.out ]; then break; fi
  sleep 0.1
done
LINE=$(cat /tmp/harness.out)
BASE=$(echo "$LINE" | awk '{print $1}')
TOK=$(echo "$LINE" | awk '{print $2}')
URL="$BASE/exec/$TOK"
echo "harness URL: $URL"

echo "=== running client-go SPDY oracle ==="
set +e
/tmp/oracle "$URL"
RC=$?
set -e
echo "=== harness stderr ==="
cat /tmp/harness.err || true
kill $HPID 2>/dev/null || true
exit $RC
