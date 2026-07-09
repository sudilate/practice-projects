#!/usr/bin/env bash
set -eo pipefail

cd "$(dirname "$0")/.."

cargo build -p core-engine
cd gateway
bun install
cd ..

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"; kill 0' EXIT

RUST_PORT=7900
GATEWAY_PORT=3002

target/debug/core-engine \
  --node-id node-1 \
  --addr "127.0.0.1:$RUST_PORT" \
  --membership-addr "127.0.0.1:$((RUST_PORT + 100))" \
  --wal "$TMPDIR/node-1.log" \
  --raft-log "$TMPDIR/node-1.raft.log" >"$TMPDIR/node.out" 2>&1 &

for _ in $(seq 1 50); do
  if grep -q 'became leader' "$TMPDIR/node.out" 2>/dev/null; then
    break
  fi
  sleep 0.1
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
  sleep 0.1
done

echo "submitting task..."
response=$(curl -s -X POST "http://127.0.0.1:$GATEWAY_PORT/tasks" \
  -H 'Content-Type: application/json' \
  -d '{"type":"uppercase","payload":"hello world"}')
echo "$response"

task_id=$(printf '%s' "$response" | sed -n 's/.*"taskId":"\([^"]*\)".*/\1/p')
if [[ -z "$task_id" ]]; then
  echo "FAIL: no taskId in response" >&2
  exit 1
fi

echo "waiting for result..."
for _ in $(seq 1 50); do
  status_response=$(curl -s "http://127.0.0.1:$GATEWAY_PORT/tasks/$task_id")
  if printf '%s' "$status_response" | grep -q '"status":"completed"'; then
    echo "$status_response"
    echo "PASS: end-to-end task execution succeeded"
    exit 0
  fi
  sleep 0.1
done

echo "FAIL: task did not complete in time" >&2
echo "last status: $status_response" >&2
exit 1
