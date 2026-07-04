# Binary Protocol

Initial frame format:

```text
[length: u32 big-endian][opcode: u8][payload: N bytes]
```

`length` is the payload length only. The full frame length is `5 + length`.

## Initial Opcodes

| Opcode | Name | Purpose |
| --- | --- | --- |
| 1 | AppendTask | Client or gateway submits a task payload. |
| 2 | Ack | Successful operation response. |
| 3 | Error | Failed operation response. |
| 4 | Ping | Membership liveness ping. |
| 5 | AckPing | Membership liveness ACK. |
| 6 | RequestVote | Raft election request. |
| 7 | AppendEntries | Raft heartbeat and log replication request. |

Streaming decode, max-frame limits, and version negotiation are Phase 1 tasks.
