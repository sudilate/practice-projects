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

```sh
cargo test
```

```sh
cd gateway
bun install
bun test
bun run typecheck
```

## Current Status

Project scaffold is in place. Phase 1 implementation starts with the WAL test suite and binary protocol hardening before the kqueue event loop is wired.
