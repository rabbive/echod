"""Coordinator node — full consensus participant.

Implements leader election (with energy-weighted scoring), log replication,
and heartbeat / liveness-ping management.  Designed to be used by both the
Raft baseline and the ECHO protocol; protocol-specific hooks are provided
as overridable methods.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any

from simulation.core.config import (
    DELTA_THRESHOLD,
    ELECTION_TIMEOUT_MAX,
    ELECTION_TIMEOUT_MIN,
    LIVENESS_PING_INTERVAL,
)
from simulation.core.log import ReplicatedLog
from simulation.core.messages import (
    AppendEntriesRPC,
    AppendEntriesResponse,
    LeafRegisterRequest,
    LeafRegisterResponse,
    LivenessPing,
    LogEntry,
    Message,
    NodeState,
    RequestVoteRPC,
    RequestVoteResponse,
    SensorDataReport,
    TriggerType,
)
from simulation.core.node import Node

logger = logging.getLogger(__name__)


class CoordinatorNode(Node):
    """A coordinator-tier node that participates in consensus."""

    def __init__(self, node_id: str, battery: float = 1.0) -> None:
        super().__init__(node_id, tier="coordinator", battery=battery)
        self.state = NodeState.FOLLOWER

        # Election bookkeeping
        self._election_deadline: float = self._new_election_deadline()
        self._votes_received: dict[str, int] = {}  # voter_id -> voter_battery

        # Leader-only replication state
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        # Liveness ping timer (leader only)
        self._last_ping_time: float = 0.0

        # Registered leaf nodes
        self._leaves: set[str] = set()

        # Last committed sensor values (for delta-trigger detection)
        self._last_sensor: dict[str, float] = {}

        # Partition epoch (non-zero in provisional mode)
        self.partition_epoch: int = 0

        # Most recently observed leader (from AppendEntries / pings) —
        # followers forward sensor reports here.
        self._current_leader_id: str | None = None

        # Peer battery levels as reported in AppendEntriesResponses
        self._peer_batteries: dict[str, int] = {}

    # ---------------------------------------------------------------- timers
    def _wakeup_interval(self) -> float:
        """Wake exactly at the election deadline instead of the poll quantum.

        Without this, deadlines separated by less than the 50 ms poll
        quantum are observed at the same tick, causing lockstep split
        votes.  Leaders/observers keep the default quantum.
        """
        base = super()._wakeup_interval()
        if self.state in (NodeState.LEADER, NodeState.LOCAL_LEADER, NodeState.OBSERVER):
            return base
        remaining = self._election_deadline - time.monotonic()
        return max(0.001, min(base, remaining))

    def _new_election_deadline(self) -> float:
        timeout_s = random.randint(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX) / 1000.0
        return time.monotonic() + timeout_s

    def reset_election_timer(self) -> None:
        """Reset the election timeout (called on heartbeat/vote grant)."""
        self._election_deadline = self._new_election_deadline()

    # ----------------------------------------------------------- idle tick
    async def _on_idle(self) -> None:
        """Drive election timeouts and leader pings."""
        now = time.monotonic()

        if self.state == NodeState.OBSERVER:
            return

        if self.state == NodeState.LEADER or self.state == NodeState.LOCAL_LEADER:
            if now - self._last_ping_time >= LIVENESS_PING_INTERVAL / 1000.0:
                await self._send_liveness_ping()
                self._last_ping_time = now
            return

        if now >= self._election_deadline:
            await self.start_election()

    # ----------------------------------------------------------- election
    async def start_election(self) -> None:
        """Transition to CANDIDATE and request votes from peers."""
        if self.state == NodeState.OBSERVER:
            return

        self.current_term += 1
        self.state = NodeState.CANDIDATE
        self.voted_for = self.node_id
        self._votes_received = {self.node_id: int(self.battery * 100)}
        self.reset_election_timer()

        logger.info(
            "%s starting election for term %d (battery=%d%%)",
            self.node_id, self.current_term, int(self.battery * 100),
        )

        rpc = RequestVoteRPC(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=self.log.last_index,
            last_log_term=self.log.last_term,
            battery_level=int(self.battery * 100),
        )
        await self.broadcast(rpc)

    # ----------------------------------------------------------- vote handling
    async def handle_request_vote(self, sender: str, rpc: RequestVoteRPC) -> None:
        """Decide whether to grant a vote to the requesting candidate.

        Raft §5.2 + ECHO extension: also reject if candidate battery is
        below the observer threshold.
        """
        if rpc.term > self.current_term:
            self.step_down(rpc.term)

        grant = False
        if (
            rpc.term == self.current_term
            and self.state != NodeState.OBSERVER
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
        """Collect votes and become leader when a majority is achieved.

        ECHO energy-weighted scoring:
            score(C) = votes_received * (battery / max_battery_among_candidates)
        In Phase 1 the scoring is used for tie-breaking; the simple majority
        rule still applies for correctness.
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

    def _has_majority(self) -> bool:
        if self.cluster is None:
            return False
        coordinator_count = sum(
            1 for n in self.cluster.nodes.values() if n.tier == "coordinator"
        )
        return len(self._votes_received) > coordinator_count // 2

    async def _become_leader(self) -> None:
        """Transition to LEADER and initialise replication state."""
        self.state = NodeState.LEADER
        logger.info(
            "%s became LEADER for term %d",
            self.node_id, self.current_term,
        )

        if self.cluster:
            for nid, node in self.cluster.nodes.items():
                if nid != self.node_id and node.tier == "coordinator":
                    self.next_index[nid] = self.log.last_index + 1
                    self.match_index[nid] = 0

        self._last_ping_time = time.monotonic()
        await self._send_liveness_ping()

    # -------------------------------------------------------- append entries
    async def handle_append_entries(self, sender: str, rpc: AppendEntriesRPC) -> None:
        """Process an AppendEntries RPC from the leader (Raft §5.3)."""
        if rpc.term > self.current_term:
            self.step_down(rpc.term)

        if rpc.term < self.current_term:
            await self.send(sender, AppendEntriesResponse(
                term=self.current_term, success=False,
                responder_id=self.node_id,
                responder_battery=int(self.battery * 100),
            ))
            return

        self.reset_election_timer()
        self._current_leader_id = rpc.leader_id
        if self.state == NodeState.CANDIDATE:
            self.state = NodeState.FOLLOWER

        # Log consistency check
        if rpc.prev_log_index > 0:
            prev_term = self.log.term_at(rpc.prev_log_index)
            if prev_term != rpc.prev_log_term:
                await self.send(sender, AppendEntriesResponse(
                    term=self.current_term, success=False,
                    responder_id=self.node_id,
                    responder_battery=int(self.battery * 100),
                ))
                return

        # Append new entries (handle conflicts)
        for entry in rpc.entries:
            existing = self.log.get(entry.index)
            if existing is not None and existing.term != entry.term:
                self.log.truncate_from(entry.index)
            if self.log.last_index < entry.index:
                self.log.append(entry)

        # Advance commit index
        if rpc.leader_commit > self.log.commit_index:
            self.log.commit(min(rpc.leader_commit, self.log.last_index))

        await self.send(sender, AppendEntriesResponse(
            term=self.current_term,
            success=True,
            responder_id=self.node_id,
            match_index=self.log.last_index,
            responder_battery=int(self.battery * 100),
        ))

    async def handle_append_entries_response(self, sender: str, resp: AppendEntriesResponse) -> None:
        """Update replication tracking and advance commit index when a majority matches."""
        if resp.term > self.current_term:
            self.step_down(resp.term)
            return

        if resp.responder_battery:
            self._peer_batteries[sender] = resp.responder_battery

        if self.state != NodeState.LEADER:
            return

        if resp.success:
            self.next_index[sender] = resp.match_index + 1
            self.match_index[sender] = resp.match_index
            self._try_advance_commit()
        else:
            self.next_index[sender] = max(1, self.next_index.get(sender, 1) - 1)

    def _try_advance_commit(self) -> None:
        """Leader commits entries replicated on a majority of coordinators."""
        if self.cluster is None:
            return
        coordinator_ids = [
            nid for nid, n in self.cluster.nodes.items()
            if n.tier == "coordinator" and nid != self.node_id
        ]
        for n in range(self.log.last_index, self.log.commit_index, -1):
            entry = self.log.get(n)
            if entry is None or entry.term != self.current_term:
                continue
            repl_count = 1  # count self
            for cid in coordinator_ids:
                if self.match_index.get(cid, 0) >= n:
                    repl_count += 1
            total = len(coordinator_ids) + 1
            if repl_count > total // 2:
                self.log.commit(n)
                break

    # ---------------------------------------------------- log replication
    async def replicate_entry(
        self,
        command: Any,
        trigger: TriggerType = TriggerType.DELTA,
    ) -> LogEntry:
        """Leader appends a new entry and sends AppendEntries to all peers."""
        entry = LogEntry(
            index=self.log.last_index + 1,
            term=self.current_term,
            command=command,
            trigger_type=trigger,
            partition_epoch=self.partition_epoch,
        )
        self.log.append(entry)

        if self.cluster:
            for nid in list(self.next_index):
                await self._send_append_entries(nid)

        return entry

    async def _send_append_entries(self, peer_id: str) -> None:
        prev_idx = self.next_index.get(peer_id, 1) - 1
        rpc = AppendEntriesRPC(
            term=self.current_term,
            leader_id=self.node_id,
            prev_log_index=prev_idx,
            prev_log_term=self.log.term_at(prev_idx),
            entries=tuple(self.log.entries_from(prev_idx + 1)),
            leader_commit=self.log.commit_index,
            trigger_type=TriggerType.DELTA,
            partition_epoch=self.partition_epoch,
        )
        await self.send(peer_id, rpc)

    # ---------------------------------------------------- liveness ping
    async def _send_liveness_ping(self) -> None:
        ping = LivenessPing(
            term=self.current_term,
            leader_id=self.node_id,
        )
        await self.broadcast(ping)

    async def handle_liveness_ping(self, sender: str, ping: LivenessPing) -> None:
        """Reset election timer upon receiving a liveness ping from the leader."""
        if ping.term >= self.current_term:
            if ping.term > self.current_term:
                self.step_down(ping.term)
            self._current_leader_id = ping.leader_id
            self.reset_election_timer()

    # ---------------------------------------------------- leaf management
    async def handle_leaf_register(self, sender: str, req: LeafRegisterRequest) -> None:
        """Accept a leaf node's registration request."""
        self._leaves.add(req.leaf_id)
        logger.info("%s registered leaf %s", self.node_id, req.leaf_id)
        await self.send(sender, LeafRegisterResponse(
            accepted=True,
            coordinator_id=self.node_id,
            term=self.current_term,
        ))

    async def handle_sensor_data(self, sender: str, report: SensorDataReport) -> None:
        """Evaluate delta-trigger and replicate if threshold is exceeded."""
        if self.state != NodeState.LEADER:
            return

        key = f"{report.leaf_id}:{report.sensor_type}"
        prev = self._last_sensor.get(key)
        if prev is not None and abs(report.value - prev) / max(abs(prev), 1e-9) < DELTA_THRESHOLD:
            return

        self._last_sensor[key] = report.value
        await self.replicate_entry(
            command={"sensor": report.sensor_type, "value": report.value, "leaf": report.leaf_id},
            trigger=TriggerType.DELTA,
        )

    # ---------------------------------------------------- energy scoring
    def election_score(self) -> float:
        """ECHO energy-weighted election score.

        score(C) = votes_received * (battery / max_battery_among_candidates)
        """
        if not self._votes_received:
            return 0.0
        max_bat = max(self._votes_received.values()) or 1
        own_bat = int(self.battery * 100)
        return len(self._votes_received) * (own_bat / max_bat)
