"""Metrics collector — hooks into the message bus and node events.

Tracks the five key metrics defined in the research plan:

- consensus_latency_ms: time from trigger to commit
- messages_per_round: total messages per consensus round
- energy_per_hour: normalised energy units consumed per node per simulated hour
- leader_changes: number of leadership transitions
- availability_pct: fraction of time the cluster was operational
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from simulation.core.messages import (
    AppendEntriesRPC,
    AppendEntriesResponse,
    Message,
    RequestVoteRPC,
    RequestVoteResponse,
    TriggerType,
)


@dataclass
class ConsensusRound:
    """Bookkeeping for a single consensus round."""
    trigger_time: float
    commit_time: float | None = None
    messages: int = 0
    trigger_type: TriggerType = TriggerType.LIVENESS


@dataclass
class MetricsCollector:
    """Accumulates protocol metrics over the simulation lifetime."""

    # Per-round tracking
    rounds: list[ConsensusRound] = field(default_factory=list)
    _active_round: ConsensusRound | None = field(default=None, repr=False)

    # Aggregate counters
    total_messages: int = 0
    leader_changes: int = 0
    _last_leader: str | None = field(default=None, repr=False)

    # Energy snapshots: node_id -> list of battery readings
    energy_snapshots: dict[str, list[float]] = field(default_factory=dict)

    # Availability tracking
    _cluster_up_time: float = 0.0
    _cluster_down_time: float = 0.0
    _last_check: float = field(default_factory=time.monotonic, repr=False)

    # ---------------------------------------------------------- hooks
    def on_message(self, msg: Message) -> None:
        """Called by the MessageBus for every delivered message."""
        self.total_messages += 1

        payload = msg.payload
        if self._active_round is not None:
            self._active_round.messages += 1

        if isinstance(payload, AppendEntriesRPC) and payload.entries:
            if self._active_round is None:
                self._active_round = ConsensusRound(
                    trigger_time=time.monotonic(),
                    trigger_type=payload.trigger_type,
                )

        if isinstance(payload, AppendEntriesResponse) and payload.success:
            if self._active_round is not None and self._active_round.commit_time is None:
                self._active_round.commit_time = time.monotonic()
                self.rounds.append(self._active_round)
                self._active_round = None

    def record_leader_change(self, new_leader: str) -> None:
        """Call whenever a node becomes leader."""
        if new_leader != self._last_leader:
            self.leader_changes += 1
            self._last_leader = new_leader

    def snapshot_energy(self, node_id: str, battery: float) -> None:
        """Record a battery reading for a node."""
        self.energy_snapshots.setdefault(node_id, []).append(battery)

    def record_availability(self, cluster_has_leader: bool) -> None:
        """Call periodically to track cluster availability."""
        now = time.monotonic()
        dt = now - self._last_check
        if cluster_has_leader:
            self._cluster_up_time += dt
        else:
            self._cluster_down_time += dt
        self._last_check = now

    # -------------------------------------------------------- computed metrics
    @property
    def consensus_latencies_ms(self) -> list[float]:
        """Latency (ms) for each completed consensus round."""
        return [
            (r.commit_time - r.trigger_time) * 1000
            for r in self.rounds
            if r.commit_time is not None
        ]

    @property
    def avg_consensus_latency_ms(self) -> float:
        lats = self.consensus_latencies_ms
        return sum(lats) / len(lats) if lats else 0.0

    @property
    def avg_messages_per_round(self) -> float:
        if not self.rounds:
            return 0.0
        return sum(r.messages for r in self.rounds) / len(self.rounds)

    @property
    def energy_per_node(self) -> dict[str, float]:
        """Total energy consumed per node (1.0 - final_battery)."""
        result: dict[str, float] = {}
        for nid, readings in self.energy_snapshots.items():
            if readings:
                result[nid] = readings[0] - readings[-1]
        return result

    @property
    def availability_pct(self) -> float:
        total = self._cluster_up_time + self._cluster_down_time
        if total == 0:
            return 100.0
        return (self._cluster_up_time / total) * 100.0

    def summary(self) -> dict[str, Any]:
        """Return a dict summarising all metrics."""
        return {
            "total_messages": self.total_messages,
            "consensus_rounds": len(self.rounds),
            "avg_consensus_latency_ms": round(self.avg_consensus_latency_ms, 3),
            "avg_messages_per_round": round(self.avg_messages_per_round, 1),
            "leader_changes": self.leader_changes,
            "availability_pct": round(self.availability_pct, 2),
            "energy_per_node": {
                k: round(v, 4) for k, v in self.energy_per_node.items()
            },
        }
