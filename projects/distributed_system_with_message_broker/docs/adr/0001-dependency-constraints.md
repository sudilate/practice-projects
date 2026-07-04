# ADR 0001: Dependency Constraints

## Status

Accepted

## Context

The project goal is to learn and implement distributed systems primitives directly: networking event loops, SWIM membership, Raft consensus, binary protocol handling, and WAL persistence.

Using production-grade libraries for these primitives would reduce implementation risk but would also bypass the learning objective.

## Decision

The Rust core engine must not depend on:

- Async runtimes such as Tokio or async-std.
- Raft, consensus, gossip, or SWIM implementation crates.
- High-level networking frameworks.
- Database engines for the core log path.

Allowed Rust dependencies:

- `libc` for direct OS bindings such as `kqueue` and `kevent`.
- Small, non-framework utilities only after an explicit ADR explains why they do not replace a learning primitive.

The Bun gateway may use framework-level HTTP libraries because its job is developer-facing API ergonomics, not learning low-level HTTP internals.

Allowed gateway dependencies:

- Bun runtime APIs.
- Fastify for HTTP routing.
- TypeScript and test tooling.

## Consequences

- The core engine will take longer to build than if it used established crates.
- More behavior must be covered by unit and integration tests.
- Networking and consensus modules must remain small, explicit, and well documented.
- New dependencies require reviewing whether they undermine the project objective.
