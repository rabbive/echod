"""Base Node class shared by Coordinator and Leaf nodes.

Provides the state machine skeleton, battery management, and the
async message-dispatch loop.  Subclasses override the handle_* methods
to implement protocol-specific behaviour.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Literal

from simulation.core.config import T_LOW, T_RESTORE
from simulation.core.log import ReplicatedLog
from simulation.core.messages import (
    AppendEntriesRPC,
    AppendEntriesResponse,
    LeafRegisterRequest,
    LeafRegisterResponse,
    LeafState,
    LivenessPing,
    Message,
    NodeState,
    RequestVoteRPC,
    RequestVoteResponse,
    SensorDataReport,
)

if TYPE_CHECKING:
    from simulation.core.cluster import Cluster

logger = logging.getLogger(__name__)


class Node:
    """Abstract base for every node in the simulation."""

    def __init__(
        self,
        node_id: str,
        tier: Literal["coordinator", "leaf"],
        battery: float = 1.0,
    ) -> None:
        self.node_id = node_id
        self.tier = tier
        self.battery = battery  # 0.0 – 1.0

        # Consensus bookkeeping
        self.current_term: int = 0
        self.voted_for: str | None = None
        self.log = ReplicatedLog()

        # State
        self.state: NodeState | LeafState = (
            NodeState.FOLLOWER if tier == "coordinator" else LeafState.UNREGISTERED
        )

        # Backref set by Cluster when the node is registered
        self.cluster: Cluster | None = None

        # Inbox fed by the message bus
        self._inbox: asyncio.Queue[Message] = asyncio.Queue()

        # Lifecycle flag
        self._running = False

    # ------------------------------------------------------------------ run
    async def run(self) -> None:
        """Main event loop — consume messages from the inbox."""
        self._running = True
        logger.info("%s started (tier=%s)", self.node_id, self.tier)
        try:
            while self._running:
                try:
                    msg: Message = await asyncio.wait_for(
                        self._inbox.get(), timeout=0.05,
                    )
                    await self.handle_message(msg)
                except asyncio.TimeoutError:
                    await self._on_idle()
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            logger.info("%s stopped", self.node_id)

    def stop(self) -> None:
        """Signal the node to exit its run-loop."""
        self._running = False

    # ----------------------------------------------------------- messaging
    def deliver(self, msg: Message) -> None:
        """Called by the message bus to place a message in this node's inbox."""
        self._inbox.put_nowait(msg)

    async def send(self, recipient_id: str, payload: Any) -> None:
        """Send a message through the cluster's message bus."""
        if self.cluster is None:
            return
        msg = Message(
            sender_id=self.node_id,
            recipient_id=recipient_id,
            payload=payload,
        )
        await self.cluster.message_bus.route(msg)

    async def broadcast(self, payload: Any, exclude_self: bool = True) -> None:
        """Broadcast a payload to every reachable node."""
        if self.cluster is None:
            return
        for nid in self.cluster.nodes:
            if exclude_self and nid == self.node_id:
                continue
            await self.send(nid, payload)

    # ------------------------------------------------------- message dispatch
    async def handle_message(self, msg: Message) -> None:
        """Dispatch an incoming message to the appropriate handler."""
        payload = msg.payload

        if isinstance(payload, RequestVoteRPC):
            await self.handle_request_vote(msg.sender_id, payload)
        elif isinstance(payload, RequestVoteResponse):
            await self.handle_request_vote_response(msg.sender_id, payload)
        elif isinstance(payload, AppendEntriesRPC):
            await self.handle_append_entries(msg.sender_id, payload)
        elif isinstance(payload, AppendEntriesResponse):
            await self.handle_append_entries_response(msg.sender_id, payload)
        elif isinstance(payload, LivenessPing):
            await self.handle_liveness_ping(msg.sender_id, payload)
        elif isinstance(payload, LeafRegisterRequest):
            await self.handle_leaf_register(msg.sender_id, payload)
        elif isinstance(payload, LeafRegisterResponse):
            await self.handle_leaf_register_response(msg.sender_id, payload)
        elif isinstance(payload, SensorDataReport):
            await self.handle_sensor_data(msg.sender_id, payload)
        else:
            logger.warning("%s received unknown payload type: %s", self.node_id, type(payload))

    # ---- Override points (no-op defaults) ----

    async def handle_request_vote(self, sender: str, rpc: RequestVoteRPC) -> None:
        """Handle an incoming RequestVote RPC."""

    async def handle_request_vote_response(self, sender: str, resp: RequestVoteResponse) -> None:
        """Handle a vote reply."""

    async def handle_append_entries(self, sender: str, rpc: AppendEntriesRPC) -> None:
        """Handle an incoming AppendEntries RPC."""

    async def handle_append_entries_response(self, sender: str, resp: AppendEntriesResponse) -> None:
        """Handle an AppendEntries reply."""

    async def handle_liveness_ping(self, sender: str, ping: LivenessPing) -> None:
        """Handle a lightweight liveness ping from the leader."""

    async def handle_leaf_register(self, sender: str, req: LeafRegisterRequest) -> None:
        """Handle a leaf node's registration request (coordinator side)."""

    async def handle_leaf_register_response(self, sender: str, resp: LeafRegisterResponse) -> None:
        """Handle the coordinator's registration response (leaf side)."""

    async def handle_sensor_data(self, sender: str, report: SensorDataReport) -> None:
        """Handle an incoming sensor data report from a leaf."""

    async def _on_idle(self) -> None:
        """Called each loop iteration when no message was received.

        Subclasses use this to drive election timeouts, liveness pings, etc.
        """

    # -------------------------------------------------------- battery helpers
    def tick_battery(self, delta: float) -> None:
        """Drain battery by *delta* (0.0–1.0 scale) and handle state transitions.

        - Below T_LOW (15 %): Coordinator transitions to OBSERVER.
        - Above T_RESTORE (25 %): OBSERVER transitions back to FOLLOWER.
        """
        self.battery = max(0.0, min(1.0, self.battery - delta))
        pct = self.battery * 100

        if self.tier != "coordinator":
            return

        if pct < T_LOW and self.state not in (NodeState.OBSERVER,):
            logger.info("%s battery %.1f%% < T_LOW — becoming OBSERVER", self.node_id, pct)
            self.state = NodeState.OBSERVER
            self.voted_for = None
        elif pct >= T_RESTORE and self.state == NodeState.OBSERVER:
            logger.info("%s battery %.1f%% >= T_RESTORE — re-entering as FOLLOWER", self.node_id, pct)
            self.state = NodeState.FOLLOWER

    def step_down(self, new_term: int) -> None:
        """Revert to follower state upon discovering a higher term."""
        self.current_term = new_term
        self.voted_for = None
        if self.tier == "coordinator" and self.state in (
            NodeState.CANDIDATE, NodeState.LEADER, NodeState.LOCAL_LEADER,
        ):
            self.state = NodeState.FOLLOWER

    def __repr__(self) -> str:
        return (
            f"Node({self.node_id!r}, tier={self.tier!r}, "
            f"state={self.state!r}, term={self.current_term}, "
            f"battery={self.battery:.0%})"
        )
