# Architecture Decision Records

Short records of significant design decisions in echod — why something was built
one way and not another. These are meant to be readable standalone (by a human or
an AI agent) without needing to reconstruct the reasoning from commit history or
the full design doc.

Each ADR follows: **Status → Context → Decision → Consequences**.

| ADR | Title |
|---|---|
| [0001](0001-tiered-architecture.md) | Tiered coordinator/leaf architecture instead of flat Raft |
| [0002](0002-battery-ordered-elections.md) | Deterministic battery-ordered election timeouts instead of energy-weighted vote scoring |
| [0003](0003-coordinators-only-consensus-traffic.md) | Consensus traffic (RequestVote/AppendEntries/pings) never touches leaves |
| [0004](0004-seeded-workload-harness.md) | Deterministic seeded workload for fair 3-way protocol comparison |

New ADRs go here as numbered files (`000N-short-title.md`), added to the table above.
