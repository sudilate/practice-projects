# Security Notes (Untrusted Clients)

This project is a learning system. Treat every network-facing surface as
**untrusted** unless you deliberately harden it.

## Trust Boundaries

| Surface | Trust assumption today |
| --- | --- |
| Gateway HTTP (`PORT`) | Anyone who can reach it can submit tasks |
| Core TCP binary port | Anyone who can reach it can append tasks / scrape metrics |
| Core UDP membership | Anyone who can reach it can inject membership gossip |
| Local WAL / raft log files | Process user can read/write durable state |

There is **no authentication, authorization, TLS, or mutual TLS**.

## Risks If Exposed Publicly

1. **Arbitrary task submission** — CPU/memory DoS via task flood.
2. **Protocol abuse** — oversized frames (capped at 1 MiB) still cost decode/buffer work.
3. **Membership spoofing** — forged SWIM messages can disturb failure detection.
4. **Raft disruption** — forged RequestVote/AppendEntries can stall progress if the attacker is on-path on the cluster network.
5. **Data disclosure** — task payloads and metrics are not encrypted on the wire.
6. **Filesystem access** — weak host permissions on `data/` expose committed logs.

## Minimum Hardening for Lab Use

- Bind to `127.0.0.1` (or a private VPC) only; do not publish ports to the internet.
- Put the gateway behind a reverse proxy with TLS and auth if humans use it.
- Run core nodes on an isolated private network; do not share that network with untrusted tenants.
- Use OS firewall rules (pf/iptables/security groups) to allow only known peer IPs for TCP/UDP cluster ports.
- Run processes as a non-root user with a dedicated data directory and tight file modes (`0700`).
- Cap host resources (ulimit / cgroup) so task floods cannot starve the machine.
- Rotate or wipe lab data directories between experiments.

## Explicit Non-Goals (Today)

- End-user identity and ACLs
- Multi-tenant isolation
- Encrypted cluster traffic
- Secure key management
- Audit logging suitable for compliance

## Reporting

If you publish a fork, document any hardening you add and never claim production
security properties without an independent review.
