#!/usr/bin/env bash
set -eo pipefail

cd "$(dirname "$0")/.."

cargo build -p core-engine
cd gateway
bun install
cd ..

TMPDIR="$(mktemp -d)"
cleanup() {
  local job
  for job in $(jobs -p 2>/dev/null); do
    kill "$job" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

RUST_PORT=8800
GATEWAY_PORT=3003
REQUESTS="${REQUESTS:-30}"

target/debug/core-engine \
  --node-id bench-gateway \
  --addr "127.0.0.1:$RUST_PORT" \
  --membership-addr "127.0.0.1:$((RUST_PORT + 100))" \
  --wal "$TMPDIR/node.log" \
  --raft-log "$TMPDIR/node.raft.log" >"$TMPDIR/node.out" 2>&1 &

for _ in $(seq 1 80); do
  if grep -q 'became leader' "$TMPDIR/node.out" 2>/dev/null; then
    break
  fi
  sleep 0.05
done

if ! grep -q 'became leader' "$TMPDIR/node.out" 2>/dev/null; then
  echo "FAIL: rust node did not become leader" >&2
  exit 1
fi

PORT=$GATEWAY_PORT CLUSTER_HOST=127.0.0.1 CLUSTER_PORT=$RUST_PORT \
  bun run gateway/src/server.ts >"$TMPDIR/gateway.out" 2>&1 &

for _ in $(seq 1 50); do
  if curl -s "http://127.0.0.1:$GATEWAY_PORT/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.05
done

if ! curl -s "http://127.0.0.1:$GATEWAY_PORT/health" >/dev/null 2>&1; then
  echo "FAIL: gateway health check failed" >&2
  exit 1
fi

# Warmup
curl -s -X POST "http://127.0.0.1:$GATEWAY_PORT/tasks" \
  -H 'Content-Type: application/json' \
  -d '{"type":"uppercase","payload":"warmup"}' >/dev/null

python3 - "$GATEWAY_PORT" "$REQUESTS" <<'PY'
import json, statistics, sys, time, urllib.request

port = int(sys.argv[1])
requests_n = int(sys.argv[2])
url = f"http://127.0.0.1:{port}/tasks"
body = json.dumps({"type": "uppercase", "payload": "hello"}).encode()

latencies = []
for i in range(requests_n):
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    started = time.perf_counter()
    with urllib.request.urlopen(req, timeout=10) as resp:
        status = resp.status
        payload = resp.read()
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    if status not in (200, 202):
        raise SystemExit(f"unexpected status {status}: {payload!r}")
    latencies.append(elapsed_ms)

latencies.sort()
p50 = latencies[len(latencies) // 2]
p95 = latencies[max(0, int(len(latencies) * 0.95) - 1)]
p99 = latencies[max(0, int(len(latencies) * 0.99) - 1)]
avg = statistics.fmean(latencies)
print(
    f"requests={requests_n} avg_ms={avg:.2f} p50_ms={p50:.2f} "
    f"p95_ms={p95:.2f} p99_ms={p99:.2f} min_ms={latencies[0]:.2f} max_ms={latencies[-1]:.2f}"
)
PY
