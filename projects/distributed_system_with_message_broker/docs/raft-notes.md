# Raft Notes

Raft implementation constraints:

- No Raft crates.
- No async runtime dependency.
- State machine logic must be independently unit-testable.
- Network transport should translate binary frames into pure Raft messages.
- WAL persistence must happen before log entries are considered durable.

Initial election target: new leader within 150-300ms after leader failure in a 5-node local cluster.

Current runtime slice uses short-lived TCP connections for Raft RPCs on each node's existing TCP listener. `RequestVote` and `AppendEntries` frame families include a one-byte kind prefix so replies can share the same opcode family as requests. Persistent peer sockets remain a later optimization.

Raft log entries (term, index, task_id, payload) are persisted to a separate `--raft-log` file using the existing WAL record format. On startup the runtime replays the durable log to reconstruct `RaftState.log` and the in-memory task state machine. This enables leader restart: a node that crashes and restarts with the same `--raft-log` path recovers its committed tasks and resumes execution.
