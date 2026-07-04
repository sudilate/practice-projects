#!/usr/bin/env bash
set -euo pipefail

trap 'kill 0' EXIT

cargo run -p core-engine -- --addr 127.0.0.1:7000 --wal data/node-1.log &
cargo run -p core-engine -- --addr 127.0.0.1:7001 --wal data/node-2.log &
cargo run -p core-engine -- --addr 127.0.0.1:7002 --wal data/node-3.log &

wait
