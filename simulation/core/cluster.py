"""Cluster orchestrator and in-process message bus.

The Cluster owns all nodes and the MessageBus, drives the simulation clock,
and provides helpers for failure / partition injection.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable

from simulation.core.config import (
    DRAIN_MULT_CANDIDATE,
    DRAIN_MULT_FOLLOWER,
    DRAIN_MULT_LEADER,
    DRAIN_MULT_LEAF,
    DRAIN_MULT_OBSERVER,
)
from simulation.core.messages import Message, NodeState
from simulation.core.node import Node

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Message bus
# --------------------------------------------------------------------------

class MessageBus:
    """In-process async message router that respects partition topology.

    Messages between nodes in different partitions are silently dropped.
    An optional hook is called for every successfully delivered message
    so the metrics collector can observe traffic.
    """

    def __init__(self) -> None:
        self._nodes: dict[str, Node] = {}
        self._partitions: list[set[str]] = []
        self.on_message: Callable[[Message], Any] | None = None
        self.total_messages: int = 0

    def register(self, node: Node) -> None:
        self._nodes[node.node_id] = node

    def set_partitions(self, partitions: list[set[str]]) -> None:
        """Define which groups of nodes can communicate with each other.

        Each set in the list is a connected partition.  Nodes not appearing
        in any set are considered isolated.
        """
        self._partitions = partitions

    def clear_partitions(self) -> None:
        self._partitions = []

    def _can_reach(self, src: str, dst: str) -> bool:
        if not self._partitions:
            return True
        for group in self._partitions:
            if src in group and dst in group:
                return True
        return False

    async def route(self, msg: Message) -> bool:
        """Deliver a message if the sender can reach the recipient.

        Returns True if the message was delivered, False if dropped.
        """
        dst = self._nodes.get(msg.recipient_id)
        if dst is None:
            return False

        if not self._can_reach(msg.sender_id, msg.recipient_id):
            return False

        dst.deliver(msg)
        self.total_messages += 1

        if self.on_message is not None:
            self.on_message(msg)

        return True


# --------------------------------------------------------------------------
# Cluster
# --------------------------------------------------------------------------

class Cluster:
    """Top-level simulation driver.

    Manages node lifecycle, failure injection, partition manipulation,
    and the discrete-event simulation loop.
    """

    def __init__(self) -> None:
        self.nodes: dict[str, Node] = {}
        self.message_bus = MessageBus()
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._failed: set[str] = set()
        self._sim_start: float = 0.0

    # -------------------------------------------------------- node management
    def add_node(self, node: Node) -> None:
        """Register a node with the cluster and message bus."""
        node.cluster = self  # type: ignore[assignment]
        self.nodes[node.node_id] = node
        self.message_bus.register(node)

    def remove_node(self, node_id: str) -> Node | None:
        node = self.nodes.pop(node_id, None)
        if node is not None:
            node.stop()
            node.cluster = None
        return node

    # ----------------------------------------------------- failure injection
    def inject_failure(self, node_id: str) -> None:
        """Simulate a crash-stop failure for a node."""
        if node_id in self._tasks:
            self._tasks[node_id].cancel()
        self._failed.add(node_id)
        node = self.nodes.get(node_id)
        if node:
            node.stop()
        logger.info("INJECTED failure on %s", node_id)

    def recover_node(self, node_id: str) -> None:
        """Bring a previously-failed node back online."""
        self._failed.discard(node_id)
        node = self.nodes.get(node_id)
        if node:
            node._running = False  # ensure clean restart
            task = asyncio.create_task(node.run())
            self._tasks[node_id] = task
            logger.info("RECOVERED node %s", node_id)

    # -------------------------------------------------- partition injection
    def inject_partition(
        self, group_a: list[str], group_b: list[str],
    ) -> None:
        """Split the network so group_a and group_b cannot communicate."""
        self.message_bus.set_partitions([set(group_a), set(group_b)])
        logger.info("INJECTED partition: %s | %s", group_a, group_b)

    def heal_partition(self) -> None:
        """Restore full connectivity."""
        self.message_bus.clear_partitions()
        logger.info("HEALED partition — full connectivity restored")

    # -------------------------------------------------------- simulation run
    async def run(self, duration_seconds: float) -> None:
        """Run the simulation for a fixed wall-clock duration.

        All non-failed nodes run concurrently as asyncio tasks.  The
        simulation stops after *duration_seconds* or when cancelled.
        """
        self._sim_start = time.monotonic()

        for nid, node in self.nodes.items():
            if nid not in self._failed:
                task = asyncio.create_task(node.run())
                self._tasks[nid] = task

        try:
            await asyncio.sleep(duration_seconds)
        finally:
            for node in self.nodes.values():
                node.stop()
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
            self._tasks.clear()

    @property
    def elapsed_seconds(self) -> float:
        return time.monotonic() - self._sim_start if self._sim_start else 0.0

    # ------------------------------------------------------- battery drain
    def tick_all_batteries(self, delta: float) -> None:
        """Drain battery on every node by *delta*."""
        for node in self.nodes.values():
            node.tick_battery(delta)

    def tick_batteries_weighted(self, base_delta: float) -> None:
        """Drain battery proportional to each node's current role.

        Leaders do the most work (pings, replication), candidates are
        mid-election, observers are nearly idle, and leaves only sense
        and report — so their idle drain differs accordingly.
        """
        for node in self.nodes.values():
            if node.tier != "coordinator":
                node.tick_battery(base_delta * DRAIN_MULT_LEAF)
                continue

            state = node.state
            if state in (NodeState.LEADER, NodeState.LOCAL_LEADER):
                mult = DRAIN_MULT_LEADER
            elif state == NodeState.CANDIDATE:
                mult = DRAIN_MULT_CANDIDATE
            elif state == NodeState.OBSERVER:
                mult = DRAIN_MULT_OBSERVER
            else:
                mult = DRAIN_MULT_FOLLOWER
            node.tick_battery(base_delta * mult)
