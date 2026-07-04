# WAL Format

Initial record format:

```text
[length: u32 big-endian][payload: N bytes]
```

The in-memory index maps logical log indexes to file offsets and payload lengths.

Durability target for Phase 1: every append flushes payload bytes with `sync_data` before returning success.

Future decisions:

- Record checksum.
- Segment rotation.
- Snapshot and compaction strategy.

## Corruption Policy

Opening a WAL validates every record from the start of the file and rebuilds the in-memory index. If the file ends with a partial record header or a partial payload, `Wal::open` fails with `InvalidData`.

The current implementation is intentionally strict: it does not silently truncate corrupt tails. That keeps early Raft durability behavior explicit while the log format is still simple.

Future recovery work can add an operator-controlled repair mode that truncates only the final partial record after reporting the original file length and last valid offset.
