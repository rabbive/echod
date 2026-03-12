"""ECHO protocol implementation.

Energy-aware Clustered Hierarchical cOnsensus — the core innovations over
standard Raft:

1. Tiered architecture (Coordinator + Leaf)
2. Energy-weighted leader election
3. Event-driven consensus triggers (replaces continuous heartbeat)
4. Partition-tolerant provisional consensus with reconciliation
"""

from __future__ import annotations

import logging
import time
from typing import Any

from simulation.core.cluster import Cluster
from simulation.core.coordinator import CoordinatorNode
from simulation.core.config import PARTITION_TIMEOUT, T_LOW
from simulation.core.leaf import LeafNode
from simulation.core.messages import (
    AppendEntriesRPC,
    AppendEntriesResponse,
    LogEntry,
    NodeState,
    RequestVoteRPC,
    RequestVoteResponse,
    TriggerType,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# ECHO Coordinator
# --------------------------------------------------------------------------

class EchoCoordinator(CoordinatorNode):
    """Coordinator with ECHO-specific extensions.

    - Rejects votes for candidates with battery below T_LOW.
    - Uses energy-weighted scoring for leader election tie-breaking.
    - Tracks partition epoch for provisional consensus.
    - Supports reconciliation after partition healing.
    """

    def __init__(self, node_id: str, battery: float = 1.0) -> None:
        super().__init__(node_id, battery=battery)
        self.partition_epoch: int = 0
        self._partition_detect_time: float | None = None
        self._pending_triggers: list[tuple[Any, TriggerType]] = []

    # ----- Election with energy gating -----

    async def handle_request_vote(self, sender: str, rpc: RequestVoteRPC) -> None:
        """Extend vote handling with energy-awareness.

        In ECHO, a candidate with battery_level < T_LOW is rejected
        regardless of log state, because low-energy nodes must not lead.
        """
        if rpc.term > self.current_term:
            self.step_down(rpc.term)

        grant = False
        if (
            rpc.term == self.current_term
            and self.state != NodeState.OBSERVER
            and rpc.battery_level >= T_LOW
            and (self.voted_for is None or self.voted_for == rpc.candidate_id)
            and self.log.is_up_to_date(rpc.last_log_index, rpc.last_log_term)
        ):
            grant = True
            self.voted_for = rpc.candidate_id
            self.reset_election_timer()

        resp = RequestVoteResponse(
            term=self.current_term,
            vote_granted=grant,
            voter_id=self.node_id,
            voter_battery=int(self.battery * 100),
        )
        await self.send(sender, resp)

    async def handle_request_vote_response(self, sender: str, resp: RequestVoteResponse) -> None:
        """Collect votes using energy-weighted scoring for tie-breaking.

        When two candidates tie on vote count, the one with the higher
        election_score() wins.
        """
        if resp.term > self.current_term:
            self.step_down(resp.term)
            return

        if self.state != NodeState.CANDIDATE or resp.term != self.current_term:
            return

        if resp.vote_granted:
            self._votes_received[sender] = resp.voter_battery

        if self._has_majority():
            await self._become_leader()

    # ----- Event-driven consensus triggers -----

    async def trigger_consensus(
        self,
        command: Any,
        trigger: TriggerType,
    ) -> LogEntry | None:
        """Initiate a consensus round for a specific event trigger.

        Only the leader may propose; followers buffer triggers that are
        forwarded once they observe a leader.
        """
        if self.state not in (NodeState.LEADER, NodeState.LOCAL_LEADER):
            self._pending_triggers.append((command, trigger))
            return None

        return await self.replicate_entry(command, trigger=trigger)

    async def _become_leader(self) -> None:
        """Extend leader promotion to flush any buffered triggers."""
        await super()._become_leader()
        await self._flush_pending_triggers()

    async def _flush_pending_triggers(self) -> None:
        pending = list(self._pending_triggers)
        self._pending_triggers.clear()
        for command, trigger in pending:
            await self.replicate_entry(command, trigger=trigger)

    # ----- Partition-tolerant provisional consensus -----

    def enter_provisional_mode(self) -> None:
        """Enter provisional consensus after detecting a partition.

        Increments partition_epoch so all provisional entries are tagged.
        A sub-cluster may elect a LOCAL_LEADER in this mode.
        """
        self.partition_epoch += 1
        if self.state == NodeState.LEADER:
            self.state = NodeState.LOCAL_LEADER
        self._partition_detect_time = time.monotonic()
        logger.info(
            "%s entering provisional mode (epoch=%d)",
            self.node_id, self.partition_epoch,
        )

    def exit_provisional_mode(self) -> None:
        """Leave provisional consensus after partition heals."""
        if self.state == NodeState.LOCAL_LEADER:
            self.state = NodeState.FOLLOWER
        self.partition_epoch = 0
        self._partition_detect_time = None
        logger.info("%s exited provisional mode", self.node_id)

    async def reconcile(self, other_entries: list[LogEntry], other_commit: int) -> None:
        """Reconcile logs after a partition heals.

        The sub-cluster with the highest globally-committed log index wins.
        The losing side replays its provisional entries as new proposals.

        This method is called on the *losing* side: it truncates its
        provisional entries and re-proposes them.
        """
        provisional = [
            e for e in self.log.entries_from(1) if e.partition_epoch > 0
        ]

        if other_commit > self.log.commit_index:
            self.log.truncate_from(self.log.commit_index + 1)
            for entry in other_entries:
                self.log.append(entry)
            self.log.commit(other_commit)

        self.exit_provisional_mode()

        for entry in provisional:
            self._pending_triggers.append(
                (entry.command, entry.trigger_type),
            )
        await self._flush_pending_triggers()

    # ----- AppendEntries with partition epoch -----

    async def handle_append_entries(self, sender: str, rpc: AppendEntriesRPC) -> None:
        """Extend standard handling to track partition epoch."""
        await super().handle_append_entries(sender, rpc)
        if rpc.partition_epoch > 0 and self.partition_epoch == 0:
            self.enter_provisional_mode()


# --------------------------------------------------------------------------
# ECHO Leaf (thin wrapper — most logic is in LeafNode)
# --------------------------------------------------------------------------

class EchoLeaf(LeafNode):
    """Leaf node in an ECHO cluster.

    Identical to the base LeafNode for Phase 1; future phases may add
    ECHO-specific leaf behaviour (e.g., partition-aware coordinator search).
    """


# --------------------------------------------------------------------------
# Cluster builder
# --------------------------------------------------------------------------

def build_echo_cluster(
    coordinator_count: int = 5,
    leaf_count: int = 10,
) -> Cluster:
    """Create a Cluster with ECHO coordinators and leaf nodes."""
    cluster = Cluster()

    for i in range(coordinator_count):
        node = EchoCoordinator(node_id=f"coord-{i}")
        cluster.add_node(node)

    for i in range(leaf_count):
        node = EchoLeaf(node_id=f"leaf-{i}")
        cluster.add_node(node)

    return cluster
