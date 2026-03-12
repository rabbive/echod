"""Standard Raft baseline implementation.

Uses the same Cluster / MessageBus infrastructure as ECHO so comparisons
are fair.  All nodes are equal peers (no tiering).  The leader sends
continuous heartbeats rather than event-driven liveness pings.
"""

from __future__ import annotations

import logging
from typing import Any

from simulation.core.cluster import Cluster
from simulation.core.coordinator import CoordinatorNode
from simulation.core.config import LIVENESS_PING_INTERVAL
from simulation.core.messages import (
    AppendEntriesRPC,
    LogEntry,
    NodeState,
    TriggerType,
)

logger = logging.getLogger(__name__)


class RaftNode(CoordinatorNode):
    """A Raft peer — every node is a full participant (no tiering).

    Inherits the election / log-replication machinery from CoordinatorNode
    and adds continuous heartbeat behaviour.
    """

    def __init__(self, node_id: str, battery: float = 1.0) -> None:
        super().__init__(node_id, battery=battery)
        self._heartbeat_interval_s = LIVENESS_PING_INTERVAL / 1000.0

    # Override idle to send full AppendEntries heartbeats (not just pings)
    async def _on_idle(self) -> None:
        """In Raft the leader sends a full AppendEntries as heartbeat."""
        import time as _time

        now = _time.monotonic()

        if self.state == NodeState.OBSERVER:
            return

        if self.state == NodeState.LEADER:
            if now - self._last_ping_time >= self._heartbeat_interval_s:
                await self._send_heartbeats()
                self._last_ping_time = now
            return

        if now >= self._election_deadline:
            await self.start_election()

    async def _send_heartbeats(self) -> None:
        """Leader sends empty AppendEntries RPCs as heartbeats to all peers."""
        for peer_id in list(self.next_index):
            await self._send_append_entries(peer_id)

    async def replicate(self, command: Any) -> LogEntry:
        """Public helper to propose a new command for replication."""
        return await self.replicate_entry(command, trigger=TriggerType.LIVENESS)


def build_raft_cluster(node_count: int = 5) -> Cluster:
    """Create a Cluster populated with *node_count* equal Raft peers."""
    cluster = Cluster()
    for i in range(node_count):
        node = RaftNode(node_id=f"raft-{i}")
        cluster.add_node(node)
    return cluster
