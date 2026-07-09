# Architecture

The system has two primary runtimes:

- Core nodes: Rust binaries that own persistence, membership, consensus, and task execution.
- API gateway: Bun and TypeScript service that exposes HTTP endpoints and speaks the binary protocol to core nodes.

## System Overview

```mermaid
flowchart LR
    Client[HTTP client]
    Gateway[Bun / Fastify gateway]
    Leader[Core node leader]
    FollowerA[Core node follower]
    FollowerB[Core node follower]
    Worker[Local worker pool]
    Wal[(WAL + Raft log)]

    Client -->|POST /tasks| Gateway
    Client -->|GET /tasks/:id| Gateway
    Gateway -->|custom TCP binary frames| Leader
    Leader -->|AppendEntries| FollowerA
    Leader -->|AppendEntries| FollowerB
    Leader -->|commit after majority ACK| Worker
    Leader --> Wal
    FollowerA --> Wal
    FollowerB --> Wal
    Worker -->|task result| Leader
    Gateway -->|GetTaskStatus| Leader
```

The gateway is intentionally thin: it validates HTTP requests, maintains cluster sockets, follows not-leader responses, and translates between JSON and the binary protocol. The Rust core owns the distributed-system behavior.

Core node modules are intentionally decoupled:

- `storage`: append-only WAL and indexing.
- `protocol`: binary frame encoding and decoding.
- `net`: raw socket event loop and transport concerns.
- `membership`: SWIM member state and dissemination.
- `raft`: consensus state machine and RPC handling.
- `task`: committed task representation and execution handoff.

## Core Node Internals

```mermaid
flowchart TD
    Tcp[Kqueue TCP event loop]
    Udp[UDP membership transport]
    Decoder[Streaming binary decoder]
    Protocol[Protocol handlers]
    Swim[SWIM membership state]
    Raft[Raft state machine]
    Task[Task state machine]
    Workers[Worker pool]
    Wal[(Append-only WAL)]
    RaftLog[(Durable Raft log)]
    Metrics[Metrics counters]
    Logs[Structured JSON logs]

    Tcp --> Decoder --> Protocol
    Protocol --> Raft
    Protocol --> Task
    Protocol --> Metrics
    Udp --> Swim
    Swim --> Metrics
    Raft --> Wal
    Raft --> RaftLog
    Raft --> Task
    Task --> Workers
    Workers --> Task
    Tcp --> Logs
    Raft --> Logs
    Swim --> Logs
```

The Raft state machine must remain testable without sockets, threads, or disk.

The SWIM membership state machine follows the same rule: membership conflict resolution, join handling, probe target selection, and ping/ACK state are pure logic. UDP transport only carries membership messages and should not own membership decisions.

## Task Submission Flow

```mermaid
sequenceDiagram
    participant C as HTTP client
    participant G as Bun gateway
    participant L as Raft leader
    participant F as Followers
    participant W as Worker pool

    C->>G: POST /tasks {type,payload}
    G->>L: AppendTask binary frame
    L->>L: Append to local Raft log
    L->>F: AppendEntries(task)
    F-->>L: AppendEntriesReply(success)
    L->>L: Commit after majority
    L->>W: Apply committed task
    W-->>L: Store task result
    L-->>G: Ack
    G-->>C: 202 {accepted, taskId}
    C->>G: GET /tasks/:id
    G->>L: GetTaskStatus binary frame
    L-->>G: TaskStatus binary frame
    G-->>C: 200 {status, output, error}
```

## Failure Handling

```mermaid
flowchart LR
    NodeDown[Node stops responding]
    SwimProbe[SWIM direct ping]
    PingReq[Indirect PingReq]
    Suspect[Mark Suspect]
    Failed[Mark Failed]
    Election[Raft election timeout]
    NewLeader[New leader elected]
    GatewayRetry[Gateway retries next node]

    NodeDown --> SwimProbe
    SwimProbe -->|no ACK| PingReq
    PingReq -->|no relayed ACK| Suspect
    Suspect --> Failed
    NodeDown --> Election
    Election --> NewLeader
    GatewayRetry -->|409 not leader or reconnect error| NewLeader
```

SWIM and Raft are separate on purpose. SWIM gives operators a membership view and failure dissemination. Raft decides write availability and commit safety.

## Persistence Boundaries

```mermaid
flowchart TD
    AppendTask[AppendTask payload]
    WalRecord[WAL record: length + payload]
    RaftEntry[Raft entry: term + index + task id + payload]
    Replay[Restart replay]
    TaskState[In-memory task state]

    AppendTask --> WalRecord
    AppendTask --> RaftEntry
    WalRecord --> Replay
    RaftEntry --> Replay
    Replay --> TaskState
```

The WAL is deliberately strict: truncated headers or payloads are rejected instead of silently repaired. Snapshot and compaction are planned in `docs/wal-compaction.md` but not automatic yet.
