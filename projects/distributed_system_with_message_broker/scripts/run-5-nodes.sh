#!/usr/bin/env bash
set -euo pipefail

trap 'kill 0' EXIT

cargo run -p core-engine -- \
  --node-id node-1 \
  --addr 127.0.0.1:7000 \
  --membership-addr 127.0.0.1:7100 \
  --raft-peer node-2=127.0.0.1:7001 \
  --raft-peer node-3=127.0.0.1:7002 \
  --raft-peer node-4=127.0.0.1:7003 \
  --raft-peer node-5=127.0.0.1:7004 \
  --wal data/node-1.log &
sleep 1
cargo run -p core-engine -- \
  --node-id node-2 \
  --addr 127.0.0.1:7001 \
  --membership-addr 127.0.0.1:7101 \
  --join 127.0.0.1:7100 \
  --raft-peer node-1=127.0.0.1:7000 \
  --raft-peer node-3=127.0.0.1:7002 \
  --raft-peer node-4=127.0.0.1:7003 \
  --raft-peer node-5=127.0.0.1:7004 \
  --wal data/node-2.log &
cargo run -p core-engine -- \
  --node-id node-3 \
  --addr 127.0.0.1:7002 \
  --membership-addr 127.0.0.1:7102 \
  --join 127.0.0.1:7100 \
  --raft-peer node-1=127.0.0.1:7000 \
  --raft-peer node-2=127.0.0.1:7001 \
  --raft-peer node-4=127.0.0.1:7003 \
  --raft-peer node-5=127.0.0.1:7004 \
  --wal data/node-3.log &
cargo run -p core-engine -- \
  --node-id node-4 \
  --addr 127.0.0.1:7003 \
  --membership-addr 127.0.0.1:7103 \
  --join 127.0.0.1:7100 \
  --raft-peer node-1=127.0.0.1:7000 \
  --raft-peer node-2=127.0.0.1:7001 \
  --raft-peer node-3=127.0.0.1:7002 \
  --raft-peer node-5=127.0.0.1:7004 \
  --wal data/node-4.log &
cargo run -p core-engine -- \
  --node-id node-5 \
  --addr 127.0.0.1:7004 \
  --membership-addr 127.0.0.1:7104 \
  --join 127.0.0.1:7100 \
  --raft-peer node-1=127.0.0.1:7000 \
  --raft-peer node-2=127.0.0.1:7001 \
  --raft-peer node-3=127.0.0.1:7002 \
  --raft-peer node-4=127.0.0.1:7003 \
  --wal data/node-5.log &

wait
