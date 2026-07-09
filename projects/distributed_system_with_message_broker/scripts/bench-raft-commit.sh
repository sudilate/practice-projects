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

REQUESTS="${REQUESTS:-50}"
PAYLOAD_SIZE="${PAYLOAD_SIZE:-32}"

bench_cluster() {
  local n="$1"
  local base="$2"
  local name_prefix="$3"

  local nodes=()
  local pids=()
  for i in $(seq 0 $((n - 1))); do
    local id="${name_prefix}-$((i + 1))"
    nodes+=("$id")
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
    pids[$i]=$!
  done

  local leader_port=""
  local waited=0
  while [[ "$waited" -lt 10000 ]]; do
    for i in $(seq 0 $((n - 1))); do
      if grep -q 'became leader' "$TMPDIR/${nodes[$i]}.out" 2>/dev/null; then
        leader_port=$((base + i))
        break 2
      fi
    done
    sleep 0.05
    waited=$((waited + 50))
  done

  if [[ -z "$leader_port" ]]; then
    echo "FAIL: no leader elected for ${n}-node cluster" >&2
    for pid in "${pids[@]}"; do
      kill "$pid" 2>/dev/null || true
    done
    return 1
  fi

  # Warmup
  N_NODES=$n python3 - "$leader_port" "$PAYLOAD_SIZE" 1 >/dev/null

  N_NODES=$n python3 - "$leader_port" "$PAYLOAD_SIZE" "$REQUESTS" <<'PY'
import os, socket, struct, sys, time, statistics

port = int(sys.argv[1])
payload_size = int(sys.argv[2])
requests = int(sys.argv[3])
n_nodes = os.environ.get("N_NODES", "?")
payload = b"x" * payload_size
frame = struct.pack(">I", len(payload)) + bytes([1]) + payload

latencies = []
for _ in range(requests):
    started = time.perf_counter()
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.sendall(frame)
    header = s.recv(5)
    if len(header) < 5:
        raise SystemExit("short header")
    length = struct.unpack(">I", header[:4])[0]
    opcode = header[4]
    body = b""
    while len(body) < length:
        chunk = s.recv(length - len(body))
        if not chunk:
            break
        body += chunk
    s.close()
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    if opcode != 2:
        raise SystemExit(f"expected ACK opcode 2, got {opcode}")
    latencies.append(elapsed_ms)

latencies.sort()
p50 = latencies[len(latencies) // 2]
p95 = latencies[max(0, int(len(latencies) * 0.95) - 1)]
p99 = latencies[max(0, int(len(latencies) * 0.99) - 1)]
avg = statistics.fmean(latencies)
print(
    f"nodes={n_nodes} requests={requests} avg_ms={avg:.2f} p50_ms={p50:.2f} "
    f"p95_ms={p95:.2f} p99_ms={p99:.2f} min_ms={latencies[0]:.2f} max_ms={latencies[-1]:.2f}"
)
PY

  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  sleep 0.2
}

echo "=== Raft commit latency: 3 nodes ==="
bench_cluster 3 8600 raft3

echo "=== Raft commit latency: 5 nodes ==="
bench_cluster 5 8700 raft5
