# Distributed Task Orchestrator and Message Broker

Fault-tolerant distributed task orchestrator and message broker built for
learning distributed systems primitives from first principles.

> **Platform note:** the Rust core engine uses macOS `kqueue` and currently runs
> only on macOS. CI and local core-node scripts assume macOS. The Bun gateway
> runs anywhere Bun is supported.

## Goals

- Rust core engine with raw `std::net`, manual event management, and macOS `kqueue` polling
- Custom append-only WAL for durable task and consensus logs
- Custom binary protocol for node-to-node and gateway-to-node traffic
- SWIM-style cluster membership without a central registry
- Raft leader election and log replication implemented from scratch
- Bun/TypeScript API gateway exposing a developer-friendly HTTP API

## Repository Layout

```text
crates/core-engine/    Rust cluster node engine
gateway/               Bun/Fastify API gateway
docs/                  Architecture, protocol, ops notes
scripts/               Local cluster, e2e, failure, and benchmark scripts
TASKS.md               Phase-by-phase execution plan
```

## Quickstart (macOS)

```sh
# Rust checks
cargo fmt --check
cargo test

# Gateway checks
cd gateway && bun install && bun test && bun run typecheck && cd ..

# One node + gateway e2e
./scripts/e2e-gateway.sh
```

Submit a task against a running gateway:

```sh
curl -s -X POST http://127.0.0.1:3000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"type":"uppercase","payload":"hello"}'
```

## Local Clusters

```sh
./scripts/run-1-node.sh
./scripts/run-3-nodes.sh
./scripts/run-5-nodes.sh
```

Manual single node:

```sh
cargo run -p core-engine -- \
  --node-id node-1 \
  --addr 127.0.0.1:7000 \
  --membership-addr 127.0.0.1:7100 \
  --data-dir data/node-1 \
  --raft-peer node-2=127.0.0.1:7001
```

Gateway env:

| Variable | Default | Meaning |
| --- | --- | --- |
| `PORT` | `3000` | HTTP listen port |
| `HOST` | `0.0.0.0` | HTTP bind host |
| `CLUSTER_HOST` | `127.0.0.1` | Core node host |
| `CLUSTER_PORT` | `7000` | Core node TCP port |
| `CLUSTER_NODES` | _(optional)_ | Comma-separated `host:port` list |

## Verification Scripts

```sh
./scripts/measure-failover.sh      # leader kill → re-election latency
./scripts/e2e-gateway.sh           # HTTP → binary → Raft → worker result
./scripts/failure-matrix.sh        # kill node/leader, malformed frame, truncated WAL, reconnect
./scripts/bench-wal.sh             # WAL append throughput
./scripts/bench-raft-commit.sh     # Raft commit latency (3 and 5 nodes)
./scripts/bench-gateway-latency.sh # gateway POST /tasks latency
```

Sample local debug-build numbers (M-series Mac, not production SLOs):

- TCP 100 clients: `successes=100 throughput_rps≈117`
- WAL append 10k×64B (fsync/record): `throughput_rps≈261`
- Raft commit p50: 3 nodes ≈27ms, 5 nodes ≈38ms
- Gateway POST p50 ≈12ms
- Leader failover ≈216ms (target 150–300ms)

## HTTP API

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Liveness |
| `GET` | `/metrics` | Prometheus-style process metrics |
| `POST` | `/tasks` | Submit task (`uppercase` \| `echo` \| `reverse`) |
| `GET` | `/tasks/:id` | Task status and result |

## Current Status

Phases **0–6 are complete**: WAL + kqueue TCP, binary protocol, SWIM membership,
Raft election/replication, gateway task path, failure matrix, and benchmarks.

Phase **7 (production readiness)** adds structured logs, metrics, config defaults,
graceful shutdown, protocol versioning, security notes, deployment guide, WAL
compaction plan, and a release checklist.

## Limitations

This is a **learning system**, not a production message broker:

- Core engine is **macOS-only** (`kqueue`)
- No TLS, authentication, or authorization
- No multi-datacenter / WAN-tuned membership
- WAL grows without automatic compaction (see `docs/wal-compaction.md`)
- Task workers are in-process and best-effort after commit
- Do not expose the gateway or core ports to untrusted networks without hardening

## Documentation

| Doc | Topic |
| --- | --- |
| [docs/architecture.md](docs/architecture.md) | Module boundaries |
| [docs/binary-protocol.md](docs/binary-protocol.md) | Frame format and opcodes |
| [docs/wal-format.md](docs/wal-format.md) | WAL record layout |
| [docs/raft-notes.md](docs/raft-notes.md) | Raft design notes |
| [docs/security.md](docs/security.md) | Threat notes for untrusted clients |
| [docs/deployment.md](docs/deployment.md) | Local and lab deployment |
| [docs/wal-compaction.md](docs/wal-compaction.md) | Snapshot / compaction plan |
| [docs/release-checklist.md](docs/release-checklist.md) | Pre-release checks |
| [docs/adr/0001-dependency-constraints.md](docs/adr/0001-dependency-constraints.md) | Dependency policy |

## License

MIT — see [LICENSE](LICENSE).
