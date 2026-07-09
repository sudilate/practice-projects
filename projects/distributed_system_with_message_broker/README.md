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

The multi-node scripts start append servers on separate TCP ports, WAL files, and UDP membership ports. Nodes can exchange SWIM `Join`/`JoinAck` messages through `node-1`; full SWIM dissemination, indirect probes, failure detection, and Raft replication are later phases.

Measure leader failover latency:

```sh
./scripts/measure-failover.sh
```

The script starts a five-node cluster, waits for a leader, kills it, and reports the time until a new leader is elected.

Run one node manually:

```sh
cargo run -p core-engine -- \
  --node-id node-1 \
  --addr 127.0.0.1:7000 \
  --membership-addr 127.0.0.1:7100 \
  --wal data/node-1.log
```

Benchmark a running node with 100 concurrent TCP clients:

```sh
cargo run -p core-engine --bin tcp_benchmark -- --addr 127.0.0.1:7000 --clients 100 --payload-size 32
```

Latest local debug-build result on this workspace: `clients=100 successes=100 failures=0 elapsed_ms=855 throughput_rps=116.87`.

## Current Status

Phase 0 is complete. Phase 1 has a strict WAL, Rust and TypeScript frame helpers, streaming decoders, max frame-size enforcement, structured error payloads, and a macOS `kqueue` TCP append server. A single Rust node accepts `AppendTask` frames, appends payloads to the WAL, and responds with `Ack` frames.

Phase 1 TCP load validation is complete for 100 concurrent clients. Phase 2 now has UDP datagram transport, Rust membership frame payload codecs, a minimal SWIM `Join`/`JoinAck` runtime, randomized direct probes, indirect `PingReq`, local `Suspect`/`Failed` transitions, piggybacked membership update dissemination, and periodic membership inspection logs. A local three-process smoke test validated discovery, shared active-member maps, and node-kill detection: after killing `node-2`, both remaining nodes logged `node-2=Failed@0`. Phase 3 has a pure Raft election state machine, Raft RPC payload codecs, TCP RequestVote/heartbeat messaging, randomized election timeouts, majority leader election, leader failover, leader WAL append, follower append validation, `next_index`/`match_index` tracking, majority-ACK commit, and application of committed entries to an in-memory task state machine (`Pending`/`Running`/`Completed`/`Failed`). Local five-process smoke tests elected a leader, re-elected a new leader after killing the first, replicated one `AppendTask` to all five node WALs before ACK (`committed entry 1 with 5 replicas`), and `scripts/measure-failover.sh` measured leader failover at 216ms, within the 150-300ms target. Remaining Raft work is full leader restart support, which requires persisting Raft term/index metadata alongside payloads in the WAL.
