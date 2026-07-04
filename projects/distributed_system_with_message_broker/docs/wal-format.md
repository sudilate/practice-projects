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
- Recovery behavior for truncated tail records.
- Snapshot and compaction strategy.
