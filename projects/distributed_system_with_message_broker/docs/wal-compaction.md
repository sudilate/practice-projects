# WAL Snapshot and Compaction Plan

## Problem

The append-only WAL and Raft log grow without bound. Restarts replay full history.
Long-running clusters will eventually:

- exhaust disk
- slow open/replay
- increase backup cost

## Goals

1. Bound on-disk growth for task/Raft history
2. Keep crash recovery correct (no lost committed entries)
3. Keep the pure Raft state machine free of I/O

## Proposed design (not fully implemented)

### Snapshots

1. After applying entry `N`, periodically serialize a snapshot:
   - last included index/term
   - task state machine map (id → status/output/error)
   - membership configuration if stored in log later
2. Write snapshot to `data/<node>/snapshot.bin` atomically (`write tmp` + `rename`).
3. Persist snapshot metadata in a small sidecar or header.

### Log truncation

1. After a durable snapshot for index `N` exists:
   - truncate prefix of Raft log / WAL for indexes `≤ N`
   - retain a small safety window if desired (`N - k`)
2. On open:
   - load snapshot
   - rebuild in-memory state
   - open remaining log suffix and replay

### Catch-up for lagging followers

1. If follower `next_index` is older than leader’s first retained index:
   - send `InstallSnapshot` RPC (new opcode family)
   - follower replaces state and resets log
2. Otherwise continue normal `AppendEntries` catch-up.

### Compaction triggers

- size threshold (e.g. WAL > 256 MiB)
- index threshold (e.g. every 10_000 applied entries)
- operator command / admin RPC

### Consistency rules

- Never truncate an entry that is not covered by a durable snapshot
- Snapshot install must be atomic w.r.t. process crash mid-write
- Applied index must never move backward after successful open

## Incremental delivery plan

1. **Docs + metrics** (this document + log size gauges) — done as planning
2. **Snapshot writer/reader** for task state only
3. **Open path**: snapshot + suffix replay
4. **Truncation** after successful snapshot
5. **InstallSnapshot** Raft RPC + follower path
6. **Operator hooks** and tests for crash mid-snapshot

## Current behavior

- Strict WAL: truncated tails are rejected (no silent repair)
- Durable Raft log via `--raft-log` / `--data-dir`
- No automatic compaction yet

Until compaction lands, operators should size disks generously and wipe lab data
directories between long experiments.
