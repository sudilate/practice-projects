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
| 8 | Join | Membership join request. |
| 9 | JoinAck | Membership join response with known members. |
| 10 | PingReq | Membership indirect ping request. |
| 11 | MembershipUpdate | Disseminated membership state update. |
| 12 | GetTaskStatus | Query a task's status and result by task ID. |
| 13 | TaskStatus | Response containing task status, output, and error. |

## Error Payload

`Error` frames use this payload shape:

```text
[code: u16 big-endian][message_length: u16 big-endian][message: UTF-8 bytes]
```

Current server behavior:

- Unsupported opcodes return `Error` with code `400`.
- Protocol decoding failures return `Error` with code `400`.
- WAL append failures return `Error` with code `500`.

## Task Status Payloads

`GetTaskStatus` payload shape:

```text
[task_id_length: u16 big-endian][task_id: UTF-8 bytes]
```

`TaskStatus` payload shape:

```text
[status: u8][output_length: u32 big-endian][output: UTF-8 bytes][error_length: u32 big-endian][error: UTF-8 bytes]
```

Status values are `0=Pending`, `1=Running`, `2=Completed`, and `3=Failed`.

## Membership Payloads

Membership frames are carried over UDP. String fields use `[length:u16_be][utf8:N]`.

Member payload shape:

```text
[id:string][addr:string][status:u8][incarnation:u64_be]
```

Status values are `0=Alive`, `1=Suspect`, `2=Failed`, and `3=Left`.

Message payloads:

- `Join`: `[member]`
- `JoinAck`: `[count:u16_be][member repeated count]`
- `Ping`: `[from:string][target:string]`
- `AckPing`: `[from:string][target:string]`
- `PingReq`: `[from:string][target:string][relay:string]`
- `MembershipUpdate`: `[member]`

Membership update dissemination is piggybacked by sending a bounded batch of `MembershipUpdate` frames alongside other outgoing membership traffic. The runtime coalesces queued updates by member ID so the newest known status is retransmitted instead of stale intermediate states.

On a direct probe timeout, the runtime sends `PingReq` to bounded relay nodes before marking the target suspect. A relay forwards `Ping` to the target on behalf of the requester; an `AckPing` from the target to the requester clears the pending probe.

## Raft Payloads

Raft RPC payloads use big-endian integers and length-prefixed strings with `[length:u16_be][utf8:N]`. The `RequestVote` opcode carries a one-byte family kind followed by either `RequestVote` (`1`) or `RequestVoteReply` (`2`). The `AppendEntries` opcode carries a one-byte family kind followed by either `AppendEntries` (`1`) or `AppendEntriesReply` (`2`).

- `RequestVote`: `[term:u64_be][candidate_id:string][last_log_index:u64_be][last_log_term:u64_be]`
- `RequestVoteReply`: `[term:u64_be][vote_granted:u8]`
- `AppendEntries`: `[term:u64_be][leader_id:string][prev_log_index:u64_be][prev_log_term:u64_be][leader_commit:u64_be][entry_count:u16_be][entry repeated count]`
- `AppendEntriesReply`: `[term:u64_be][success:u8][match_index:u64_be]`

Raft log entry payload shape:

```text
[term:u64_be][index:u64_be][task_id:string][payload_length:u32_be][payload:N]
```

## Streaming Decode

Rust and TypeScript both include streaming decoders that retain partial bytes until a complete frame is available. Multiple frames coalesced in one socket read are emitted in order.

Version negotiation remains a future production-readiness task.
