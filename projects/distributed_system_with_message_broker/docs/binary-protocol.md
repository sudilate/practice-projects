# Binary Protocol

Initial frame format:

```text
[length: u32 big-endian][opcode: u8][payload: N bytes]
```

`length` is the payload length only. The full frame length is `5 + length`.

The maximum payload length is `1 MiB`.

## Opcode Registry

| Opcode | Name | Purpose |
| --- | --- | --- |
| 1 | AppendTask | Client or gateway submits a task payload. |
| 2 | Ack | Successful operation response. |
| 3 | Error | Failed operation response. |
| 4 | Ping | Membership liveness ping. |
| 5 | AckPing | Membership liveness ACK. |
| 6 | RequestVote | Raft election request. |
| 7 | AppendEntries | Raft heartbeat and log replication request. |

## Error Payload

`Error` frames use this payload shape:

```text
[code: u16 big-endian][message_length: u16 big-endian][message: UTF-8 bytes]
```

Current server behavior:

- Unsupported opcodes return `Error` with code `400`.
- Protocol decoding failures return `Error` with code `400`.
- WAL append failures return `Error` with code `500`.

## Streaming Decode

Rust and TypeScript both include streaming decoders that retain partial bytes until a complete frame is available. Multiple frames coalesced in one socket read are emitted in order.

Version negotiation remains a future production-readiness task.
