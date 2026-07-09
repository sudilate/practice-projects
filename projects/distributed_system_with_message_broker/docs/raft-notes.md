# Raft Notes

Raft implementation constraints:

- No Raft crates.
- No async runtime dependency.
- State machine logic must be independently unit-testable.
- Network transport should translate binary frames into pure Raft messages.
- WAL persistence must happen before log entries are considered durable.

Initial election target: new leader within 150-300ms after leader failure in a 5-node local cluster.

Current runtime slice uses short-lived TCP connections for Raft RPCs on each node's existing TCP listener. `RequestVote` and `AppendEntries` frame families include a one-byte kind prefix so replies can share the same opcode family as requests. Persistent peer sockets remain a later optimization.
