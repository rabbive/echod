"""echoD — a Raft/ECHO hybrid that is more efficient than either parent.

Keeps ECHO's tiered architecture, energy gating, and partition-tolerant
provisional consensus, and adds six efficiency optimizations:

1. Edge-side delta filtering — leaves suppress sub-threshold readings
   instead of transmitting them (zero radio cost for filtered events).
2. Batched event-driven consensus — the leader coalesces all triggers
   arriving within BATCH_WINDOW_MS into a *single* log entry, so a burst
   of k events costs one consensus round instead of k rounds.
3. Coordinators-only adaptive liveness — the leader pings coordinators
   only (never leaves), backing off exponentially while idle and snapping
   back to the fast interval on consensus activity.  Each coordinator
   keepalives its own leaves at a slow fixed rate.
4. Battery-ordered election timeouts — timeout grows as battery shrinks,
   so the highest-battery node always nominates itself first and wins on
   the first ballot (no split votes, no wasted RequestVote rounds, and
   the energy-optimal leader for free).
5. Leader handoff — below T_HANDOFF the leader nominates the highest-
   battery successor directly (TimeoutNow-style) instead of waiting for
   a randomized election: one message, no availability gap.
6. Batched reconciliation — provisional entries replay as one batch
   entry after a partition heals, not one round per entry.

Consensus traffic (RequestVote / AppendEntries / pings) never touches
leaf nodes at all — leaves only talk to their own coordinator.
"""

from __future__ import annotations

import logging
import time
import zlib
from typing import Any

from simulation.core.cluster import Cluster
from simulation.core.config import (
    BATCH_WINDOW_MS,
    DELTA_THRESHOLD,
    ECHOD_ELECTION_TIMEOUT_MAX,
    ECHOD_ELECTION_TIMEOUT_MIN,
    ECHOD_PING_MAX_INTERVAL,
    ELECTION_TIE_BREAK_MS,
    LEAF_KEEPALIVE_INTERVAL,
    LIVENESS_BACKOFF_FACTOR,
    LIVENESS_PING_INTERVAL,
    MAX_BATCH_SIZE,
    T_HANDOFF,
)
from simulation.core.leaf import LeafNode
from simulation.core.messages import (
    LeadershipHandoff,
    LivenessPing,
    NodeState,
    RequestVoteRPC,
    SensorDataReport,
    TriggerType,
)
from simulation.protocols.echo import EchoCoordinator

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# echoD Leaf — edge-side delta filtering (optimization 1)
# --------------------------------------------------------------------------

class EchoDLeaf(LeafNode):
    """Leaf that filters readings at the edge before transmitting.

    A reading is only sent when it deviates from the last *transmitted*
    value by at least DELTA_THRESHOLD.  Sub-threshold changes cost zero
    messages — the filter lives on the leaf, not the coordinator.
    """

    def __init__(
        self,
        node_id: str,
        sensor_type: str = "temperature",
        battery: float = 1.0,
        auto_report: bool = True,
    ) -> None:
        super().__init__(
            node_id, sensor_type=sensor_type, battery=battery,
            auto_report=auto_report,
        )
        self._last_transmitted: float | None = None
        self.suppressed_count: int = 0

    async def _send_reading(self, value: float) -> None:
        """Transmit only when the delta threshold is breached."""
        if self._last_transmitted is not None:
            delta = abs(value - self._last_transmitted) / max(
                abs(self._last_transmitted), 1e-9,
            )
            if delta < DELTA_THRESHOLD:
                self.suppressed_count += 1
                return
        self._last_transmitted = value
        await super()._send_reading(value)


# --------------------------------------------------------------------------
# echoD Coordinator
# --------------------------------------------------------------------------

class EchoDCoordinator(EchoCoordinator):
    """Coordinator implementing the echoD efficiency optimizations."""

    def __init__(self, node_id: str, battery: float = 1.0) -> None:
        # Deterministic per-node tie-break for battery-ordered timeouts
        # (crc32 is stable across runs, unlike hash()).  Must be set
        # BEFORE super().__init__(), which calls _new_election_deadline().
        self._tie_break_ms = zlib.crc32(node_id.encode()) % ELECTION_TIE_BREAK_MS

        super().__init__(node_id, battery=battery)

        # Batching state (optimization 2)
        self._batch_buffer: list[Any] = []
        self._batch_deadline: float | None = None

        # Adaptive liveness state (optimization 3)
        self._ping_interval_ms: float = float(LIVENESS_PING_INTERVAL)
        self._last_consensus_activity: float = time.monotonic()
        self._last_leaf_keepalive: float = 0.0

        # Handoff state (optimization 5)
        self._handoff_sent: bool = False

    # ---------------------------------------------------- wakeup precision
    def _wakeup_interval(self) -> float:
        """Leaders wake at the next batch deadline or ping time.

        Keeps the 50 ms batch window and adaptive ping interval precise
        instead of rounding them to the inbox poll quantum.
        """
        base = super()._wakeup_interval()
        if self.state not in (NodeState.LEADER, NodeState.LOCAL_LEADER):
            return base

        now = time.monotonic()
        wake = base
        if self._batch_deadline is not None:
            wake = min(wake, max(0.001, self._batch_deadline - now))
        next_ping = self._last_ping_time + self._ping_interval_ms / 1000.0
        wake = min(wake, max(0.001, next_ping - now))
        return wake

    # -------------------------------------------------- election timeouts
    def _new_election_deadline(self) -> float:
        """Battery-ordered timeout (optimization 4).

        timeout = MIN + (1 - battery) * spread + deterministic tie-break

        The highest-battery coordinator always times out first, broadcasts
        RequestVote before anyone else is candidate, and wins on the first
        ballot.  Ties in battery are broken by a stable per-node offset,
        so split votes (and their wasted message rounds) do not occur.
        """
        spread = ECHOD_ELECTION_TIMEOUT_MAX - ECHOD_ELECTION_TIMEOUT_MIN
        timeout_ms = (
            ECHOD_ELECTION_TIMEOUT_MIN
            + (1.0 - self.battery) * spread
            + self._tie_break_ms
        )
        return time.monotonic() + timeout_ms / 1000.0

    async def start_election(self) -> None:
        """As base, but RequestVote goes to coordinators only.

        Leaves never participate in consensus, so they never see
        consensus traffic in echoD.
        """
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
        if self.cluster:
            for nid, node in self.cluster.nodes.items():
                if nid != self.node_id and node.tier == "coordinator":
                    await self.send(nid, rpc)

    # -------------------------------------------------------- idle tick
    async def _on_idle(self) -> None:
        """Drive batching, adaptive pings, handoff, and leaf keepalives."""
        now = time.monotonic()

        if self.state == NodeState.OBSERVER:
            return

        if self.state in (NodeState.LEADER, NodeState.LOCAL_LEADER):
            # Flush the batch when its window closes (optimization 2)
            if self._batch_deadline is not None and now >= self._batch_deadline:
                await self._flush_batch()

            # Adaptive coordinators-only ping (optimization 3)
            if now - self._last_ping_time >= self._ping_interval_ms / 1000.0:
                await self._send_liveness_ping()
                self._last_ping_time = now

            # Directed handoff when battery runs low (optimization 5)
            if int(self.battery * 100) < T_HANDOFF and not self._handoff_sent:
                await self._initiate_handoff()

        elif now >= self._election_deadline:
            await self.start_election()

        # Every coordinator keepalives *its own* leaves at a slow fixed
        # rate (optimization 3) — leaves registered to a non-leader no
        # longer flap between ACTIVE and SEARCHING.
        if (
            self._leaves
            and now - self._last_leaf_keepalive >= LEAF_KEEPALIVE_INTERVAL / 1000.0
        ):
            await self._send_leaf_keepalives()
            self._last_leaf_keepalive = now

    # ------------------------------------------- adaptive liveness (opt 3)
    async def _send_liveness_ping(self) -> None:
        """Ping coordinators only, with exponential backoff while idle."""
        ping = LivenessPing(term=self.current_term, leader_id=self.node_id)
        if self.cluster:
            for nid, node in self.cluster.nodes.items():
                if nid != self.node_id and node.tier == "coordinator":
                    await self.send(nid, ping)

        # Snap back to the fast interval if consensus was active since the
        # previous ping; otherwise back off exponentially up to the cap.
        if self._last_consensus_activity >= self._last_ping_time:
            self._ping_interval_ms = float(LIVENESS_PING_INTERVAL)
        else:
            self._ping_interval_ms = min(
                self._ping_interval_ms * LIVENESS_BACKOFF_FACTOR,
                float(ECHOD_PING_MAX_INTERVAL),
            )

    async def _send_leaf_keepalives(self) -> None:
        """Send a slow liveness ping to this coordinator's own leaves."""
        ping = LivenessPing(term=self.current_term, leader_id=self.node_id)
        for leaf_id in list(self._leaves):
            await self.send(leaf_id, ping)

    # -------------------------------------------------- batching (opt 2)
    async def handle_sensor_data(self, sender: str, report: SensorDataReport) -> None:
        """Leader buffers delta-breaching readings into the current batch.

        The edge filter in EchoDLeaf means most sub-threshold readings
        never arrive; the coordinator-side check remains as a backstop
        for forwarded reports.  Non-leaders forward to the leader as in
        ECHO.
        """
        if self.state not in (NodeState.LEADER, NodeState.LOCAL_LEADER):
            if (
                self._current_leader_id is not None
                and self._current_leader_id != self.node_id
            ):
                await self.send(self._current_leader_id, report)
            return

        key = f"{report.leaf_id}:{report.sensor_type}"
        prev = self._last_sensor.get(key)
        if prev is not None and abs(report.value - prev) / max(abs(prev), 1e-9) < DELTA_THRESHOLD:
            return
        self._last_sensor[key] = report.value

        self._batch_buffer.append({
            "sensor": report.sensor_type,
            "value": report.value,
            "leaf": report.leaf_id,
        })
        if self._batch_deadline is None:
            self._batch_deadline = time.monotonic() + BATCH_WINDOW_MS / 1000.0

        # Full batches flush immediately — no added latency under load.
        if len(self._batch_buffer) >= MAX_BATCH_SIZE:
            await self._flush_batch()

    async def _flush_batch(self) -> None:
        """Replicate all buffered readings as a single log entry."""
        if not self._batch_buffer:
            self._batch_deadline = None
            return

        commands = list(self._batch_buffer)
        self._batch_buffer.clear()
        self._batch_deadline = None

        if len(commands) == 1:
            command: Any = commands[0]
        else:
            command = {"batch": commands, "count": len(commands)}

        self._last_consensus_activity = time.monotonic()
        await self.replicate_entry(command, trigger=TriggerType.DELTA)

    # ------------------------------------------------- handoff (opt 5)
    async def _initiate_handoff(self) -> None:
        """Nominate the highest-battery successor and step down.

        One directed message replaces a full randomized election round,
        and leadership moves without an election-timeout availability gap.
        """
        self._handoff_sent = True
        if self.cluster is None:
            return

        own_battery = int(self.battery * 100)
        candidates = [
            (nid, self._peer_batteries.get(nid, 100))
            for nid, n in self.cluster.nodes.items()
            if n.tier == "coordinator"
            and nid != self.node_id
            and getattr(n, "state", None) != NodeState.OBSERVER
        ]
        if not candidates:
            return

        # Highest battery first; node_id tie-break for determinism.
        candidates.sort(key=lambda nb: (-nb[1], nb[0]))
        successor, successor_battery = candidates[0]

        if successor_battery <= own_battery:
            return  # no better candidate — remain leader

        logger.info(
            "%s battery %d%% < T_HANDOFF — handing off to %s (%d%%)",
            self.node_id, own_battery, successor, successor_battery,
        )
        await self.send(successor, LeadershipHandoff(
            term=self.current_term,
            leader_id=self.node_id,
        ))
        self.state = NodeState.FOLLOWER
        self.reset_election_timer()

    async def handle_leadership_handoff(
        self, sender: str, handoff: LeadershipHandoff,
    ) -> None:
        """Nominated successor: start an election immediately."""
        if self.state == NodeState.OBSERVER:
            return
        if handoff.term < self.current_term:
            return
        if handoff.term > self.current_term:
            self.step_down(handoff.term)

        logger.info(
            "%s accepted handoff nomination from %s",
            self.node_id, handoff.leader_id,
        )
        await self.start_election()

    # ------------------------------------ batched reconciliation (opt 6)
    async def _flush_pending_triggers(self) -> None:
        """Replay buffered triggers as ONE batch entry, not k rounds.

        Applies both to triggers buffered while leaderless and to
        provisional entries replayed during reconciliation.
        """
        if not self._pending_triggers:
            return

        pending = list(self._pending_triggers)
        self._pending_triggers.clear()
        self._last_consensus_activity = time.monotonic()

        if len(pending) == 1:
            command, trigger = pending[0]
            await self.replicate_entry(command, trigger=trigger)
            return

        await self.replicate_entry(
            {"batch": [c for c, _ in pending], "count": len(pending)},
            trigger=pending[0][1],
        )

    # ------------------------------------------------------- promotion
    async def _become_leader(self) -> None:
        """Reset echoD per-term state, then promote as usual."""
        self._handoff_sent = False
        self._ping_interval_ms = float(LIVENESS_PING_INTERVAL)
        await super()._become_leader()


# --------------------------------------------------------------------------
# Cluster builder
# --------------------------------------------------------------------------

def build_echod_cluster(
    coordinator_count: int = 5,
    leaf_count: int = 10,
    auto_report: bool = True,
) -> Cluster:
    """Create a Cluster with echoD coordinators and leaf nodes."""
    cluster = Cluster()

    for i in range(coordinator_count):
        node = EchoDCoordinator(node_id=f"coord-{i}")
        cluster.add_node(node)

    for i in range(leaf_count):
        node = EchoDLeaf(node_id=f"leaf-{i}", auto_report=auto_report)
        cluster.add_node(node)

    return cluster
