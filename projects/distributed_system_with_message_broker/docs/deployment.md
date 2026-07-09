# Deployment Guide

Lab-oriented deployment for local machines and private networks. Not a cloud
marketplace recipe.

## Prerequisites

- macOS (core engine requires `kqueue`)
- Rust toolchain (`rustc` ≥ 1.78)
- Bun (gateway)
- Python 3 (benchmark/failure scripts)

## Single-node lab

```sh
cargo build -p core-engine --release

./target/release/core-engine \
  --node-id node-1 \
  --addr 127.0.0.1:7000 \
  --membership-addr 127.0.0.1:7100 \
  --data-dir data/node-1
```

In another terminal:

```sh
cd gateway
bun install
PORT=3000 CLUSTER_HOST=127.0.0.1 CLUSTER_PORT=7000 bun run start
```

Smoke test:

```sh
curl -s http://127.0.0.1:3000/health
curl -s -X POST http://127.0.0.1:3000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"type":"uppercase","payload":"hello"}'
curl -s http://127.0.0.1:3000/metrics
```

## Multi-node lab (3 nodes)

Use `./scripts/run-3-nodes.sh` for a quick start, or launch processes with distinct:

- `--node-id`
- `--addr` / `--membership-addr`
- `--data-dir`
- `--raft-peer` entries for every other node
- `--join` pointing at node-1 membership for SWIM bootstrap

Gateway multi-node:

```sh
CLUSTER_NODES=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 bun run start
```

## Configuration reference

### Core engine CLI / env

| Flag | Env | Default |
| --- | --- | --- |
| `--node-id` | `NODE_ID` | `node-1` |
| `--addr` | `ADDR` | `127.0.0.1:7000` |
| `--membership-addr` | `MEMBERSHIP_ADDR` | `127.0.0.1:7100` |
| `--data-dir` | `DATA_DIR` | _(none)_ → sets `wal.log` + `raft.log` under dir |
| `--wal` | `WAL_PATH` | `data/node-1.log` |
| `--raft-log` | `RAFT_LOG_PATH` | optional |
| `--join` | — | SWIM join targets |
| `--raft-peer id=host:port` | — | Raft peer list |

Signals: `SIGINT` / `SIGTERM` stop the event loop gracefully.

### Gateway env

| Env | Default |
| --- | --- |
| `HOST` | `0.0.0.0` |
| `PORT` | `3000` |
| `CLUSTER_HOST` | `127.0.0.1` |
| `CLUSTER_PORT` | `7000` |
| `CLUSTER_NODES` | optional `host:port,host:port,...` |
| `LOG_LEVEL` | `info` |

## Process supervision

- Prefer a process manager (launchd, supervisord, systemd on a Mac-adjacent host) that restarts on crash.
- Mount durable disks for `--data-dir`.
- Capture stdout JSON logs from the core engine and Fastify logs from the gateway.

## Health and metrics

- Gateway: `GET /health`, `GET /metrics`
- Core: binary opcode `GetMetrics` (15) returns Prometheus text; also structured JSON logs on stdout

## Rollback / wipe

Stop processes, archive or delete the data directory, restart with empty logs.
There is no automatic cross-version migration tooling yet.

## Security

See [security.md](security.md). Default binds suitable only for private labs.
