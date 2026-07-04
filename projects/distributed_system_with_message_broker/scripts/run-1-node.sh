#!/usr/bin/env bash
set -euo pipefail

cargo run -p core-engine -- --addr 127.0.0.1:7000 --wal data/node-1.log
