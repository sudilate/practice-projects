# Task Roadmap

This file is the execution source of truth. Do not advance to the next phase until the current phase success criteria pass with tests and manual verification.

## 0. Project Setup

- [x] Initialize monorepo structure.
- [x] Add Rust workspace and `core-engine` crate.
- [x] Add Bun/Fastify gateway package.
- [x] Add placeholder binary protocol in Rust and TypeScript.
- [x] Add initial docs and task roadmap.
- [x] Add CI workflow for `cargo test`, `bun test`, and `bun run typecheck`.
- [x] Add architecture decision records for dependency constraints.
- [x] Add local development scripts for running 1, 3, and 5 node clusters.

Exit criteria:

- [x] Fresh clone can run Rust tests.
- [x] Fresh clone can install gateway dependencies and run Bun tests.
- [x] README explains project goals, layout, and commands.

## 1. Storage and Raw Networking Foundation

Goal: one Rust node accepts bytes, persists them, reads them back, and responds over a custom protocol using OS-level network polling.

### 1.1 Write-Ahead Log

- [x] Write tests for empty WAL creation.
- [x] Write tests for appending one record.
- [x] Write tests for appending multiple records.
- [x] Write tests for rebuilding an in-memory index from an existing `.log` file.
- [x] Write tests for rejecting truncated records.
- [x] Implement record format: `[length:u32_be][payload:N]`.
- [x] Implement append with `sync_data` or explicit durability mode.
- [x] Implement indexed retrieval by logical log index.
- [x] Add corruption handling policy to `docs/wal-format.md`.

### 1.2 Custom kqueue Event Loop

- [x] Write a minimal non-blocking TCP listener test harness.
- [x] Create safe wrapper around `libc::kqueue` and `libc::kevent`.
- [x] Register listener socket read readiness.
- [x] Accept non-blocking client sockets.
- [x] Register client read/write readiness.
- [x] Implement per-connection read buffers.
- [x] Implement per-connection write queues.
- [x] Add graceful close and error cleanup.
- [x] Benchmark with 100 concurrent `nc` or scripted TCP clients.

### 1.3 Binary Protocol

- [x] Define initial frame shape: `[length:u32_be][opcode:u8][payload:N]`.
- [x] Add Rust frame encode/decode tests.
- [x] Add TypeScript frame encode/decode tests.
- [x] Add streaming decoder that handles partial frames.
- [x] Add max frame size.
- [x] Add error response frame payload format.
- [x] Document opcode registry in `docs/binary-protocol.md`.

Phase 1 success criteria:

- [x] Rust binary accepts TCP connections.
- [x] Incoming frames are parsed.
- [x] Payloads are written to the WAL and flushed.
- [x] Node responds with an ACK frame.
- [x] 100+ concurrent connections complete successfully on M1 macOS.

## 2. Cluster Membership with SWIM

Goal: nodes discover peers dynamically and converge on member status without a central registry.

### 2.1 UDP Transport

- [x] Add UDP socket support to event loop.
- [x] Add datagram receive path.
- [x] Add datagram send path.
- [x] Add UDP frame opcodes for membership messages.
- [x] Add Rust payload codec for membership frames.

### 2.2 SWIM Protocol

- [x] Write pure state-machine tests for join handling.
- [x] Write pure state-machine tests for direct ping ACK.
- [x] Write pure state-machine tests for indirect ping failure.
- [x] Write pure state-machine tests for incarnation conflict resolution.
- [x] Implement member map with `Alive`, `Suspect`, `Failed`, and `Left` states.
- [x] Implement runtime direct probe loop with local suspect and failed transitions.
- [x] Implement randomized peer selection.
- [x] Implement direct `Ping` and `Ack`.
- [x] Implement indirect `PingReq`.
- [x] Implement piggybacked dissemination queue.
- [x] Add operator command or log output to inspect membership.

Phase 2 success criteria:

- [x] Three Rust binaries on separate ports discover each other.
- [x] Each node maintains an accurate shared active-member map.
- [x] Killing one node updates remaining maps within seconds.

## 3. Consensus Layer with Raft

Goal: elect a leader and replicate committed task log entries to a majority.

### 3.1 State Machine and RPCs

- [x] Write tests for follower initial state.
- [x] Write test for follower starting an election.
- [x] Write tests for vote granting and rejection.
- [x] Write tests for stale term rejection.
- [x] Write tests for AppendEntries heartbeat handling.
- [x] Write tests for log consistency checks.
- [x] Define `RequestVote` binary payload.
- [x] Define `AppendEntries` binary payload.
- [x] Keep Raft state machine independent from network I/O.

### 3.2 Leader Election

- [x] Implement randomized election timeouts.
- [x] Implement candidate vote requests.
- [x] Implement majority vote calculation.
- [x] Implement candidate-to-leader transition.
- [x] Implement leader heartbeat loop.
- [x] Implement demotion on higher term.

### 3.3 Log Replication

- [x] Implement leader append to local WAL.
- [x] Implement follower append validation.
- [x] Implement `next_index` and `match_index` tracking.
- [x] Commit only after majority ACK.
- [ ] Apply committed entries to the task state machine.
- [ ] Add failure tests for dropped follower and leader restart.

Phase 3 success criteria:

- [x] Five-node cluster elects one leader.
- [x] Task sent to leader replicates to at least three nodes before success.
- [ ] Killing leader triggers new election within 150-300ms.

## 4. API Gateway and Task Execution

Goal: external clients submit JSON tasks to Bun gateway and receive task results backed by replicated Rust consensus.

### 4.1 Bun Gateway

- [x] Initialize Bun/Fastify server.
- [x] Add `GET /health`.
- [x] Add placeholder `POST /tasks`.
- [ ] Add request validation schema.
- [ ] Add structured error responses.
- [ ] Add integration tests for HTTP routes.

### 4.2 Gateway-to-Cluster Client

- [x] Add TypeScript binary frame helpers.
- [ ] Maintain persistent `Bun.connect()` sockets to cluster nodes.
- [ ] Track current leader.
- [ ] Retry on not-leader responses.
- [ ] Handle reconnect backoff.
- [ ] Add request correlation IDs.

### 4.3 Task Dispatch

- [ ] Push committed tasks into local worker queue.
- [ ] Implement worker thread pool.
- [ ] Store task result by task ID.
- [ ] Add result retrieval path.
- [ ] Add idempotency handling for duplicate task IDs.

Phase 4 success criteria:

- [ ] `curl POST /tasks` sends JSON to Bun gateway.
- [ ] Gateway sends binary frame to Rust leader.
- [ ] Leader replicates and commits task.
- [ ] Worker executes task.
- [ ] Client receives HTTP 200 with result.

## 5. Testing Matrix

- [ ] Rust unit tests for protocol, WAL, SWIM state, and Raft state.
- [ ] Rust integration tests for single-node TCP flow.
- [ ] Multi-process tests for 3-node SWIM convergence.
- [ ] Multi-process tests for 5-node Raft election.
- [ ] Bun unit tests for protocol and cluster client.
- [ ] Bun HTTP route tests.
- [ ] Failure tests for killed node, killed leader, malformed frame, truncated WAL, and reconnect.

## 6. Benchmarking

- [ ] Benchmark WAL append throughput.
- [x] Benchmark single-node TCP throughput.
- [x] Benchmark 100 concurrent client connections.
- [ ] Benchmark Raft commit latency for 3 and 5 nodes.
- [ ] Benchmark gateway request latency.

## 7. Production Readiness

- [ ] Add structured logs.
- [ ] Add metrics endpoint.
- [ ] Add configurable ports and data directories.
- [ ] Add graceful shutdown.
- [ ] Add snapshot or WAL compaction plan.
- [ ] Add protocol versioning.
- [ ] Add security notes for untrusted clients.
- [ ] Add deployment guide.
- [ ] Add release checklist.

## 8. Final Push Checklist

- [ ] `cargo fmt --check` passes.
- [ ] `cargo test` passes.
- [ ] `bun test` passes.
- [ ] `bun run typecheck` passes.
- [ ] All docs match implemented behavior.
- [ ] Phase success criteria are demonstrated in README or docs.
- [ ] Git status contains only intentional files.
