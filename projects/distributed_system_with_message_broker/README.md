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

Run the gateway end-to-end smoke test against a single Rust node:

```sh
./scripts/e2e-gateway.sh
```

The script starts a Rust node and the Bun gateway, submits a task via `POST /tasks`, and polls `GET /tasks/:id` until the worker result is available.

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

Run the failure matrix (killed node/leader, malformed frame, truncated WAL, reconnect):

```sh
./scripts/failure-matrix.sh
```

Benchmarks:

```sh
./scripts/bench-wal.sh
./scripts/bench-raft-commit.sh
./scripts/bench-gateway-latency.sh
```

Latest local debug-build samples:

- WAL append (`records=10000 payload_size=64`): `throughput_rps=260.52` (fsync per record)
- Raft commit latency: 3 nodes `p50_ms=26.69`; 5 nodes `p50_ms=38.10`
- Gateway `POST /tasks` latency: `p50_ms=11.51`

## Current Status

Phase 0 is complete. Phase 1 has a strict WAL, Rust and TypeScript frame helpers, streaming decoders, max frame-size enforcement, structured error payloads, and a macOS `kqueue` TCP append server. A single Rust node accepts `AppendTask` frames, appends payloads to the WAL, and responds with `Ack` frames.

Phase 1 TCP load validation is complete for 100 concurrent clients. Phase 2 now has UDP datagram transport, Rust membership frame payload codecs, a minimal SWIM `Join`/`JoinAck` runtime, randomized direct probes, indirect `PingReq`, local `Suspect`/`Failed` transitions, piggybacked membership update dissemination, and periodic membership inspection logs. A local three-process smoke test validated discovery, shared active-member maps, and node-kill detection: after killing `node-2`, both remaining nodes logged `node-2=Failed@0`. Phase 3 has a pure Raft election state machine, Raft RPC payload codecs, TCP RequestVote/heartbeat messaging, randomized election timeouts, majority leader election, leader failover, leader WAL append, follower append validation, `next_index`/`match_index` tracking, majority-ACK commit, application of committed entries to an in-memory task state machine (`Pending`/`Running`/`Completed`/`Failed`), and a durable Raft log (`--raft-log`) that replays term/index/task_id/payload metadata on restart. Local five-process smoke tests elected a leader, re-elected a new leader after killing the first, replicated one `AppendTask` to all five node WALs before ACK (`committed entry 1 with 5 replicas`), and `scripts/measure-failover.sh` measured leader failover at 216ms, within the 150-300ms target. A manual leader restart test verified that a task submitted before the kill was still queryable after restart.

Phase 4 connects the Bun gateway to the Rust cluster end-to-end. The gateway validates `POST /tasks` requests with zod, exposes `GET /tasks/:id` for results, and returns structured error responses. A `ClusterClient` maintains persistent `Bun.connect()` sockets to cluster nodes, tracks the current leader, and follows `409 not raft leader` responses to the real leader. The Rust engine runs a worker thread pool that executes committed tasks (`uppercase`, `echo`, `reverse`) and stores results by task ID. `scripts/e2e-gateway.sh` demonstrates the full flow: `curl POST /tasks` → gateway → Rust leader → Raft commit → worker execution → `curl GET /tasks/:id` returns the completed result.

Phase 5/6 testing and benchmarking are complete: `scripts/failure-matrix.sh` covers killed non-leader, killed leader, malformed frames, truncated WAL open rejection, and post-restart reconnect; unit/integration coverage includes TCP Error-400 responses and gateway reconnect failover. Benchmark scripts measure WAL append throughput, Raft commit latency (3/5 nodes), and gateway request latency.
