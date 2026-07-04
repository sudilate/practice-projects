# Distributed Task Orchestrator and Message Broker

Fault-tolerant distributed task orchestrator and message broker built for learning distributed systems primitives from first principles.

## Goals

- Rust core engine with raw `std::net`, manual event management, and macOS `kqueue` polling.
- Custom append-only WAL for durable task and consensus logs.
- Custom binary protocol for node-to-node and gateway-to-node traffic.
- SWIM-style cluster membership without a central registry.
- Raft leader election and log replication implemented from scratch.
- Bun and TypeScript API gateway exposing a developer-friendly HTTP API.

## Repository Layout

```text
crates/core-engine/    Rust cluster node engine
gateway/               Bun/Fastify API gateway
docs/                  Architecture and protocol notes
TASKS.md               Phase-by-phase execution plan
```

## Commands

Run Rust checks:

```sh
cargo fmt --check
cargo test
```

Run gateway checks:

```sh
cd gateway
bun install
bun test
bun run typecheck
```

Run local core nodes:

```sh
./scripts/run-1-node.sh
./scripts/run-3-nodes.sh
./scripts/run-5-nodes.sh
```

The multi-node scripts currently start independent append servers on separate ports and WAL files. SWIM discovery and Raft replication are later phases.

Run one node manually:

```sh
cargo run -p core-engine -- --addr 127.0.0.1:7000 --wal data/node-1.log
```

## Current Status

Phase 0 is complete. Phase 1 has a strict WAL, Rust and TypeScript frame helpers, streaming decoders, max frame-size enforcement, structured error payloads, and a macOS `kqueue` TCP append server. A single Rust node accepts `AppendTask` frames, appends payloads to the WAL, and responds with `Ack` frames.

Remaining Phase 1 work is load validation with 100+ concurrent clients and benchmarking before moving to SWIM membership.
