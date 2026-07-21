# ADR 0001: Tiered coordinator/leaf architecture instead of flat Raft

**Status:** Accepted

## Context

Standard Raft treats every node as an equal peer that both votes on consensus decisions
and produces application data. In IoT deployments, most nodes are cheap sensors
(ESP32-class) that report readings but have no business participating in leader
election or log replication — giving them a vote adds consensus overhead without
adding safety value, and a sensor flapping on and off shouldn't be able to disrupt
an election.

## Decision

ECHO (and echoD after it) splits nodes into two tiers:

- **Coordinators** — full consensus participants (`CoordinatorNode` in `simulation/core/coordinator.py`); vote, hold the replicated log, can become leader.
- **Leaves** — lightweight sensor reporters (`LeafNode` in `simulation/core/leaf.py`); register with a coordinator, report data, never vote and never see consensus RPCs directly.

`MAX_COORDINATORS` caps the voting set (default reflected in `--coordinators`, typically 5), while leaf count scales independently (`--leaves`, typically 10) without affecting quorum size.

## Consequences

- Quorum math stays cheap (O(coordinator count), not O(total nodes)) regardless of how many sensors are deployed.
- A single misbehaving/dying leaf can never cause a spurious election or split vote.
- Adds a registration protocol (`LeafRegisterRequest`/`Response`) and a leaf → coordinator routing concern that flat Raft doesn't need.
- Leaves depend on their registered coordinator being reachable; this is what partition-tolerance (ADR-driven provisional consensus) and, in echoD, per-coordinator leaf keepalives (see `docs/ECHOD_VS_RAFT_ECHO.md` optimization 3) exist to handle.
