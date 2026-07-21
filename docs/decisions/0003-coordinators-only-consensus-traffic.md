# ADR 0003: Consensus traffic never touches leaves

**Status:** Accepted

## Context

In ECHO, the leader's liveness ping was broadcast to **every** node in the cluster,
coordinators and leaves alike. Measurement showed this was ECHO's single largest cost:
75–80% of its total message traffic in a 5-second run was liveness pings, most of them
to leaves that have no vote and no stake in leader liveness. Broadcasting to leaves
also caused a secondary bug: leaves registered to a *non-leader* coordinator only
received pings when that non-leader happened to be the one broadcasting (it wasn't),
so they periodically timed out and flapped between ACTIVE and SEARCHING state even
though nothing was actually wrong.

## Decision

In echoD, the rule is absolute: **RequestVote, AppendEntries, and liveness pings are
coordinator-to-coordinator only.** Leaves never see any of this traffic. Instead:

- The leader pings coordinators only (`EchoDCoordinator._send_liveness_ping`), with exponential backoff (`LIVENESS_BACKOFF_FACTOR`) up to `ECHOD_PING_MAX_INTERVAL` while idle.
- Each coordinator separately keepalives its *own* registered leaves at a slow, fixed rate (`LEAF_KEEPALIVE_INTERVAL`, `_send_leaf_keepalives`) — regardless of whether that coordinator is the leader.

## Consequences

- Removes the largest single source of message volume in ECHO by construction — leaf count no longer affects consensus-traffic volume at all.
- Fixes the leaf-flapping bug as a side effect: leaves now get a keepalive from their own coordinator regardless of cluster leadership.
- Slightly increases total message *types* (two separate keepalive mechanisms instead of one broadcast), but each is cheaper and scoped to only the nodes that need it.
- Establishes a hard invariant for any future echoD change: if you're about to send `RequestVoteRPC`, `AppendEntriesRPC`, or a leader `LivenessPing` to a leaf, that's a bug — leaves only ever exchange `LeafRegisterRequest/Response`, `SensorDataReport`, and the separate leaf-keepalive ping with their own coordinator.
