#!/usr/bin/env bash
set -eo pipefail

cd "$(dirname "$0")/.."

cargo build -p core-engine --bin wal_benchmark
cargo run -q -p core-engine --bin wal_benchmark -- \
  --records "${RECORDS:-10000}" \
  --payload-size "${PAYLOAD_SIZE:-64}"
