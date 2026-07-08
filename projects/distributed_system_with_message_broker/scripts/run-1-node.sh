#!/usr/bin/env bash
set -euo pipefail

cargo run -p core-engine -- \
  --node-id node-1 \
  --addr 127.0.0.1:7000 \
  --membership-addr 127.0.0.1:7100 \
  --wal data/node-1.log
