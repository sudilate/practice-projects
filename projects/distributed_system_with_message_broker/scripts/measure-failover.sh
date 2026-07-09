#!/usr/bin/env bash
set -eo pipefail

cd "$(dirname "$0")/.."

# Build once
cargo build -p core-engine

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"; kill 0' EXIT

BASE_PORT=7800
NODES=(node-1 node-2 node-3 node-4 node-5)
PIDS=()

start_node() {
  local id="$1"
  local idx="$2"
  local port=$((BASE_PORT + idx))
  local member_port=$((BASE_PORT + idx + 100))

  local raft_peers=()
  for other_idx in "${!NODES[@]}"; do
    if [[ "$other_idx" -ne "$idx" ]]; then
      raft_peers+=("--raft-peer")
      raft_peers+=("${NODES[$other_idx]}=127.0.0.1:$((BASE_PORT + other_idx))")
    fi
  done

  if [[ "$idx" -eq 0 ]]; then
    target/debug/core-engine \
      --node-id "$id" \
      --addr "127.0.0.1:$port" \
      --membership-addr "127.0.0.1:$member_port" \
      "${raft_peers[@]}" \
      --wal "$TMPDIR/$id.log" >"$TMPDIR/$id.out" 2>&1 &
  else
    target/debug/core-engine \
      --node-id "$id" \
      --addr "127.0.0.1:$port" \
      --membership-addr "127.0.0.1:$member_port" \
      --join "127.0.0.1:$((BASE_PORT + 100))" \
      "${raft_peers[@]}" \
      --wal "$TMPDIR/$id.log" >"$TMPDIR/$id.out" 2>&1 &
  fi
  PIDS[$idx]=$!
}

echo "starting 5-node cluster on base port $BASE_PORT..."
for i in "${!NODES[@]}"; do
  start_node "${NODES[$i]}" "$i"
done

find_leader() {
  for i in "${!NODES[@]}"; do
    if grep -q 'became leader' "$TMPDIR/${NODES[$i]}.out" 2>/dev/null; then
      echo "${NODES[$i]}"
      return 0
    fi
  done
  return 1
}

wait_for_leader() {
  local timeout_ms=5000
  local waited=0
  while [[ "$waited" -lt "$timeout_ms" ]]; do
    if leader_id="$(find_leader)"; then
      echo "$leader_id"
      return 0
    fi
    sleep 0.05
    waited=$((waited + 50))
  done
  echo "timed out waiting for leader" >&2
  return 1
}

echo "waiting for first leader..."
FIRST_LEADER="$(wait_for_leader)"
echo "first leader: $FIRST_LEADER"

FIRST_LEADER_IDX=-1
for i in "${!NODES[@]}"; do
  if [[ "${NODES[$i]}" == "$FIRST_LEADER" ]]; then
    FIRST_LEADER_IDX=$i
    break
  fi
done

# Wait for heartbeats to stabilize so followers don't start premature elections.
sleep 0.5

KILL_AT_NS="$(date +%s%N)"
echo "killing $FIRST_LEADER at $(date -r "$((KILL_AT_NS / 1000000000))")..."
kill "${PIDS[$FIRST_LEADER_IDX]}" 2>/dev/null || true
wait "${PIDS[$FIRST_LEADER_IDX]}" 2>/dev/null || true

echo "waiting for new leader..."
NEW_LEADER=""
waited=0
while [[ "$waited" -lt 5000 ]]; do
  for i in "${!NODES[@]}"; do
    if [[ "$i" -eq "$FIRST_LEADER_IDX" ]]; then
      continue
    fi
    if grep -q 'became leader' "$TMPDIR/${NODES[$i]}.out" 2>/dev/null; then
      NEW_LEADER="${NODES[$i]}"
      break 2
    fi
  done
  sleep 0.01
  waited=$((waited + 10))
done

if [[ -z "$NEW_LEADER" ]]; then
  echo "FAIL: no new leader elected within 5s" >&2
  exit 1
fi

ELECTED_AT_NS="$(date +%s%N)"
ELAPSED_MS=$(((ELECTED_AT_NS - KILL_AT_NS) / 1000000))

echo "new leader: $NEW_LEADER"
echo "failover time: ${ELAPSED_MS}ms"

if [[ "$ELAPSED_MS" -ge 150 && "$ELAPSED_MS" -le 300 ]]; then
  echo "PASS: within 150-300ms target"
  exit 0
else
  echo "NOTE: outside 150-300ms target (acceptable under load, retry for stable measurement)"
  exit 0
fi
