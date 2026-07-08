# Architecture

The system has two primary runtimes:

- Core nodes: Rust binaries that own persistence, membership, consensus, and task execution.
- API gateway: Bun and TypeScript service that exposes HTTP endpoints and speaks the binary protocol to core nodes.

Core node modules are intentionally decoupled:

- `storage`: append-only WAL and indexing.
- `protocol`: binary frame encoding and decoding.
- `net`: raw socket event loop and transport concerns.
- `membership`: SWIM member state and dissemination.
- `raft`: consensus state machine and RPC handling.
- `task`: committed task representation and execution handoff.

The Raft state machine must remain testable without sockets, threads, or disk.

The SWIM membership state machine follows the same rule: membership conflict resolution, join handling, probe target selection, and ping/ACK state are pure logic. UDP transport only carries membership messages and should not own membership decisions.
