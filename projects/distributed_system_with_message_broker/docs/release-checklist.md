# Release Checklist

Use this before tagging or publishing a new revision.

## Code health

- [ ] `cargo fmt --check`
- [ ] `cargo test`
- [ ] `cd gateway && bun install && bun test && bun run typecheck`
- [ ] No unintended files in `git status` (`target/`, `node_modules/`, `data/`, `.env`)

## Behavioral smoke

- [ ] `./scripts/e2e-gateway.sh`
- [ ] `./scripts/failure-matrix.sh`
- [ ] `./scripts/measure-failover.sh` (note failover ms)
- [ ] Optional benches: `./scripts/bench-wal.sh`, `./scripts/bench-raft-commit.sh`, `./scripts/bench-gateway-latency.sh`

## Docs

- [ ] README platform caveat (macOS core) is accurate
- [ ] README commands match scripts and CLI flags
- [ ] `docs/binary-protocol.md` opcode table matches code
- [ ] `docs/security.md` and `docs/deployment.md` reviewed for this release
- [ ] `LICENSE` present
- [ ] Gateway dependency versions are pinned (no `latest`)

## Release notes

- [ ] Summarize user-visible changes
- [ ] Call out breaking protocol/CLI changes
- [ ] List known limitations (no TLS/auth, macOS-only core, no compaction)

## Publish

- [ ] Tag version if standalone (`v0.x.y`) or monorepo path note
- [ ] Push branch/tag
- [ ] Confirm CI green on macOS runners
