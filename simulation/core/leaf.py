"""Leaf node — lightweight observer that reports sensor data.

Leaf nodes never participate in consensus voting.  They register with a
coordinator, stream sensor readings, and receive committed decisions.
If the coordinator becomes unreachable they enter SEARCHING state.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any

from simulation.core.config import PARTITION_TIMEOUT, SAMPLE_INTERVAL
from simulation.core.messages import (
    AppendEntriesRPC,
    LeafRegisterRequest,
    LeafRegisterResponse,
    LeafState,
    LivenessPing,
    SensorDataReport,
)
from simulation.core.node import Node

logger = logging.getLogger(__name__)


class LeafNode(Node):
    """A leaf-tier node that observes consensus and reports sensor data."""

    def __init__(
        self,
        node_id: str,
        sensor_type: str = "temperature",
        battery: float = 1.0,
        auto_report: bool = True,
    ) -> None:
        super().__init__(node_id, tier="leaf", battery=battery)
        self.state = LeafState.UNREGISTERED

        self.sensor_type = sensor_type
        self.coordinator_id: str | None = None

        # When False the leaf only reports readings injected by the
        # workload generator (deterministic cross-protocol comparison).
        self.auto_report = auto_report

        # Timing
        self._last_coordinator_contact: float = time.monotonic()
        self._last_sample_time: float = 0.0
        self._coordinator_timeout_s = PARTITION_TIMEOUT / 1000.0

        # Simulated sensor value
        self._sensor_value: float = 20.0 + random.uniform(-2, 2)

    # --------------------------------------------------------- idle tick
    async def _on_idle(self) -> None:
        now = time.monotonic()

        if self.state == LeafState.UNREGISTERED:
            await self._try_register()
            return

        if self.state == LeafState.ACTIVE:
            if now - self._last_coordinator_contact > self._coordinator_timeout_s:
                logger.info("%s coordinator timeout — entering SEARCHING", self.node_id)
                self.state = LeafState.SEARCHING
                self.coordinator_id = None
                return

            if (
                self.auto_report
                and now - self._last_sample_time >= SAMPLE_INTERVAL / 1000.0
            ):
                await self._report_sensor()
                self._last_sample_time = now

        elif self.state == LeafState.SEARCHING:
            await self._try_register()

    # ------------------------------------------------------- registration
    async def _try_register(self) -> None:
        """Send a registration request to a random coordinator."""
        if self.cluster is None:
            return

        coordinators = [
            nid for nid, n in self.cluster.nodes.items()
            if n.tier == "coordinator"
        ]
        if not coordinators:
            return

        target = random.choice(coordinators)
        req = LeafRegisterRequest(
            leaf_id=self.node_id,
            capabilities={"sensor": self.sensor_type},
        )
        await self.send(target, req)

    async def handle_leaf_register_response(self, sender: str, resp: LeafRegisterResponse) -> None:
        """Process the coordinator's registration reply."""
        if resp.accepted:
            self.coordinator_id = resp.coordinator_id
            self.state = LeafState.ACTIVE
            self._last_coordinator_contact = time.monotonic()
            logger.info("%s registered with coordinator %s", self.node_id, resp.coordinator_id)

    # ------------------------------------------------------- sensor data
    async def _report_sensor(self) -> None:
        """Sample the (simulated) sensor and send the reading."""
        self._sensor_value += random.uniform(-0.5, 0.5)
        await self._send_reading(self._sensor_value)

    async def _send_reading(self, value: float) -> None:
        """Transmit a reading to the registered coordinator.

        Split out from _report_sensor so subclasses (echoD) can apply
        edge-side filtering and the workload generator can inject exact
        values through the normal path.
        """
        if self.coordinator_id is None:
            return

        report = SensorDataReport(
            leaf_id=self.node_id,
            sensor_type=self.sensor_type,
            value=value,
        )
        await self.send(self.coordinator_id, report)

    async def inject_reading(self, value: float) -> None:
        """Workload-generator hook: report an exact reading (no jitter)."""
        self._sensor_value = value
        await self._send_reading(value)

    # ---------------------------------------------------- incoming RPCs
    async def handle_append_entries(self, sender: str, rpc: AppendEntriesRPC) -> None:
        """Leaf passively applies committed entries (observe only)."""
        if self.state == LeafState.ACTIVE and sender == self.coordinator_id:
            self._last_coordinator_contact = time.monotonic()
            for entry in rpc.entries:
                self.log.append(entry)
            if rpc.leader_commit > self.log.commit_index:
                self.log.commit(min(rpc.leader_commit, self.log.last_index))

    async def handle_liveness_ping(self, sender: str, ping: LivenessPing) -> None:
        """Track coordinator liveness from pings."""
        if self.state == LeafState.ACTIVE and sender == self.coordinator_id:
            self._last_coordinator_contact = time.monotonic()
