#!/usr/bin/env bash
set -eo pipefail

cd "$(dirname "$0")/.."

cargo build -p core-engine

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

BASE_PORT=8000
PASS=0
FAIL=0

pass() {
  echo "PASS: $1"
  PASS=$((PASS + 1))
}

fail() {
  echo "FAIL: $1" >&2
  FAIL=$((FAIL + 1))
}

start_cluster() {
  local n="$1"
  local base="$2"
  local name_prefix="$3"
  NODES=()
  PIDS=()
  for i in $(seq 0 $((n - 1))); do
    local id="${name_prefix}-$((i + 1))"
    NODES+=("$id")
    local port=$((base + i))
    local member_port=$((base + i + 100))
    local raft_peers=()
    for j in $(seq 0 $((n - 1))); do
      if [[ "$j" -ne "$i" ]]; then
        raft_peers+=("--raft-peer" "${name_prefix}-$((j + 1))=127.0.0.1:$((base + j))")
      fi
    done
    if [[ "$i" -eq 0 ]]; then
      target/debug/core-engine \
        --node-id "$id" \
        --addr "127.0.0.1:$port" \
        --membership-addr "127.0.0.1:$member_port" \
        "${raft_peers[@]}" \
        --wal "$TMPDIR/$id.log" \
        --raft-log "$TMPDIR/$id.raft.log" >"$TMPDIR/$id.out" 2>&1 &
    else
      target/debug/core-engine \
        --node-id "$id" \
        --addr "127.0.0.1:$port" \
        --membership-addr "127.0.0.1:$member_port" \
        --join "127.0.0.1:$((base + 100))" \
        "${raft_peers[@]}" \
        --wal "$TMPDIR/$id.log" \
        --raft-log "$TMPDIR/$id.raft.log" >"$TMPDIR/$id.out" 2>&1 &
    fi
    PIDS[$i]=$!
  done
}

find_leader_idx() {
  local n="${#NODES[@]}"
  for i in $(seq 0 $((n - 1))); do
    if grep -q 'became leader' "$TMPDIR/${NODES[$i]}.out" 2>/dev/null; then
      echo "$i"
      return 0
    fi
  done
  return 1
}

wait_for_leader() {
  local timeout_ms="${1:-8000}"
  local waited=0
  while [[ "$waited" -lt "$timeout_ms" ]]; do
    if idx="$(find_leader_idx)"; then
      echo "$idx"
      return 0
    fi
    sleep 0.05
    waited=$((waited + 50))
  done
  return 1
}

send_append() {
  local port="$1"
  local payload="$2"
  python3 - "$port" "$payload" <<'PY'
import socket, struct, sys
port = int(sys.argv[1])
payload = sys.argv[2].encode()
frame = struct.pack(">I", len(payload)) + bytes([1]) + payload
s = socket.create_connection(("127.0.0.1", port), timeout=5)
s.sendall(frame)
header = s.recv(5)
if len(header) < 5:
    raise SystemExit("short response header")
length = struct.unpack(">I", header[:4])[0]
opcode = header[4]
body = b""
while len(body) < length:
    chunk = s.recv(length - len(body))
    if not chunk:
        break
    body += chunk
s.close()
print(f"{opcode}:{length}")
PY
}

echo "=== 1. killed non-leader node ==="
start_cluster 3 "$BASE_PORT" "killnode"
if LEADER_IDX="$(wait_for_leader)"; then
  FOLLOWER_IDX=$(( (LEADER_IDX + 1) % 3 ))
  LEADER_PORT=$((BASE_PORT + LEADER_IDX))
  kill "${PIDS[$FOLLOWER_IDX]}" 2>/dev/null || true
  wait "${PIDS[$FOLLOWER_IDX]}" 2>/dev/null || true
  sleep 0.3
  if out="$(send_append "$LEADER_PORT" "after-kill-follower")"; then
    opcode="${out%%:*}"
    if [[ "$opcode" == "2" ]]; then
      pass "killed non-leader; leader still ACKs"
    else
      fail "killed non-leader; unexpected opcode $opcode"
    fi
  else
    fail "killed non-leader; append failed"
  fi
else
  fail "killed non-leader; no leader elected"
fi
kill "${PIDS[@]}" 2>/dev/null || true
wait 2>/dev/null || true
sleep 0.2

echo "=== 2. killed leader ==="
BASE_PORT=8100
start_cluster 5 "$BASE_PORT" "killleader"
if LEADER_IDX="$(wait_for_leader)"; then
  kill "${PIDS[$LEADER_IDX]}" 2>/dev/null || true
  wait "${PIDS[$LEADER_IDX]}" 2>/dev/null || true
  # clear leader markers for survivors by waiting for a *new* became leader line after kill
  sleep 0.05
  NEW_LEADER=""
  waited=0
  while [[ "$waited" -lt 5000 ]]; do
    for i in $(seq 0 4); do
      if [[ "$i" -eq "$LEADER_IDX" ]]; then
        continue
      fi
      if grep -q 'became leader' "$TMPDIR/${NODES[$i]}.out" 2>/dev/null; then
        NEW_LEADER=$i
        break 2
      fi
    done
    sleep 0.02
    waited=$((waited + 20))
  done
  if [[ -n "$NEW_LEADER" ]]; then
    NEW_PORT=$((BASE_PORT + NEW_LEADER))
    sleep 0.2
    if out="$(send_append "$NEW_PORT" "after-kill-leader")"; then
      opcode="${out%%:*}"
      if [[ "$opcode" == "2" ]]; then
        pass "killed leader; new leader ACKs"
      else
        fail "killed leader; unexpected opcode $opcode"
      fi
    else
      fail "killed leader; append to new leader failed"
    fi
  else
    fail "killed leader; no new leader elected"
  fi
else
  fail "killed leader; initial leader missing"
fi
kill "${PIDS[@]}" 2>/dev/null || true
wait 2>/dev/null || true
sleep 0.2

echo "=== 3. malformed frame ==="
BASE_PORT=8200
target/debug/core-engine \
  --node-id node-malformed \
  --addr "127.0.0.1:$BASE_PORT" \
  --membership-addr "127.0.0.1:$((BASE_PORT + 100))" \
  --wal "$TMPDIR/malformed.log" >"$TMPDIR/malformed.out" 2>&1 &
MALFORMED_PID=$!
for _ in $(seq 1 50); do
  if grep -q 'listening on' "$TMPDIR/malformed.out" 2>/dev/null; then
    break
  fi
  sleep 0.05
done
if python3 - "$BASE_PORT" <<'PY'; then
import socket, struct, sys
port = int(sys.argv[1])
s = socket.create_connection(("127.0.0.1", port), timeout=5)
s.sendall(bytes([0, 0, 0, 0, 99]))  # unknown opcode
header = s.recv(5)
length = struct.unpack(">I", header[:4])[0]
opcode = header[4]
body = s.recv(length)
s.close()
if opcode != 3:
    raise SystemExit(f"expected Error opcode 3, got {opcode}")
code = struct.unpack(">H", body[:2])[0]
if code != 400:
    raise SystemExit(f"expected error code 400, got {code}")
print("ok")
PY
  pass "malformed frame returns Error 400"
else
  fail "malformed frame handling"
fi
kill "$MALFORMED_PID" 2>/dev/null || true
wait "$MALFORMED_PID" 2>/dev/null || true

echo "=== 4. truncated WAL ==="
TRUNC="$TMPDIR/truncated.log"
printf '\x00\x00' >"$TRUNC"
if target/debug/core-engine \
  --node-id trunc \
  --addr "127.0.0.1:8300" \
  --membership-addr "127.0.0.1:8400" \
  --wal "$TRUNC" >"$TMPDIR/trunc.out" 2>&1; then
  fail "truncated WAL: engine started unexpectedly"
else
  if ! grep -q 'listening on' "$TMPDIR/trunc.out"; then
    pass "truncated WAL rejected on open"
  else
    fail "truncated WAL: unexpected engine output"
  fi
fi

echo "=== 5. reconnect after node restart ==="
BASE_PORT=8500
target/debug/core-engine \
  --node-id reconnect-node \
  --addr "127.0.0.1:$BASE_PORT" \
  --membership-addr "127.0.0.1:$((BASE_PORT + 100))" \
  --wal "$TMPDIR/reconnect.log" \
  --raft-log "$TMPDIR/reconnect.raft.log" >"$TMPDIR/reconnect.out" 2>&1 &
RECONNECT_PID=$!
for _ in $(seq 1 80); do
  if grep -q 'became leader' "$TMPDIR/reconnect.out" 2>/dev/null; then
    break
  fi
  sleep 0.05
done
if ! grep -q 'became leader' "$TMPDIR/reconnect.out" 2>/dev/null; then
  fail "reconnect: node never became leader"
else
  if ! out="$(send_append "$BASE_PORT" "before-restart")" || [[ "${out%%:*}" != "2" ]]; then
    fail "reconnect: initial append failed"
  else
    kill "$RECONNECT_PID" 2>/dev/null || true
    wait "$RECONNECT_PID" 2>/dev/null || true
    target/debug/core-engine \
      --node-id reconnect-node \
      --addr "127.0.0.1:$BASE_PORT" \
      --membership-addr "127.0.0.1:$((BASE_PORT + 100))" \
      --wal "$TMPDIR/reconnect2.log" \
      --raft-log "$TMPDIR/reconnect2.raft.log" >"$TMPDIR/reconnect2.out" 2>&1 &
    RECONNECT_PID=$!
    for _ in $(seq 1 80); do
      if grep -q 'became leader' "$TMPDIR/reconnect2.out" 2>/dev/null; then
        break
      fi
      sleep 0.05
    done
    if out="$(send_append "$BASE_PORT" "after-restart")" && [[ "${out%%:*}" == "2" ]]; then
      pass "reconnect after node restart succeeds"
    else
      fail "reconnect after node restart failed"
    fi
    kill "$RECONNECT_PID" 2>/dev/null || true
    wait "$RECONNECT_PID" 2>/dev/null || true
  fi
fi

echo
echo "failure matrix: $PASS passed, $FAIL failed"
if [[ "$FAIL" -ne 0 ]]; then
  exit 1
fi
