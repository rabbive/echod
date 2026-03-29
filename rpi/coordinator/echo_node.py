"""ECHO coordinator node for Raspberry Pi deployment.

Implements the full ECHO consensus protocol over MQTT transport with
real (or mock) battery monitoring.  Can be launched as a standalone
process — one per Raspberry Pi in the physical cluster, or several on
a single laptop for a hardware-free demo.

Usage::

    python -m rpi.coordinator.echo_node \
        --node-id coord-0 \
        --peers coord-1,coord-2,coord-3,coord-4 \
        --mock
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from rpi.config import (
    BROKER_HOST,
    BROKER_PORT,
    CLUSTER_ID,
    DELTA_THRESHOLD,
    ELECTION_TIMEOUT_MAX,
    ELECTION_TIMEOUT_MIN,
    LIVENESS_PING_INTERVAL,
    T_LOW,
    T_RESTORE,
)
from rpi.coordinator.battery import BatteryMonitor
from rpi.coordinator.transport import MQTTTransport

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

class NodeState(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"
    OBSERVER = "observer"
    LOCAL_LEADER = "local_leader"


@dataclass
class LogEntry:
    index: int
    term: int
    command: Any
    trigger_type: str = "delta"
    partition_epoch: int = 0
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "term": self.term,
            "command": self.command,
            "trigger_type": self.trigger_type,
            "partition_epoch": self.partition_epoch,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, d: dict) -> LogEntry:
        return cls(**{k: d[k] for k in cls.__dataclass_fields__ if k in d})


# ---------------------------------------------------------------------------
# Coordinator
# ---------------------------------------------------------------------------

class EchoCoordinator:
    """ECHO protocol coordinator running over MQTT.

    The coordinator participates in leader election, log replication,
    and liveness management.  Energy-aware vote gating rejects candidates
    whose battery has dropped below ``T_LOW``.
    """

    def __init__(
        self,
        node_id: str,
        peers: list[str],
        transport: MQTTTransport,
        battery: BatteryMonitor,
    ) -> None:
        self.node_id = node_id
        # Peers must exclude ourselves; otherwise quorum math and replication
        # fanout are incorrect in multi-coordinator deployments (including the
        # local hardware-free demo which passes a full peer list).
        self.peers = [p for p in peers if p != node_id]
        self.transport = transport
        self.battery = battery

        # Consensus state
        self.state = NodeState.FOLLOWER
        self.current_term: int = 0
        self.voted_for: str | None = None
        self.log: list[LogEntry] = []
        self.commit_index: int = 0

        # Election bookkeeping
        self._votes: dict[str, int] = {}
        self._election_deadline: float = self._new_election_deadline()

        # Leader-only replication state
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        # Liveness
        self._last_ping_time: float = 0.0

        # Leaf management
        self._leaves: set[str] = set()
        self._last_sensor: dict[str, float] = {}

        # Partition epoch (non-zero in provisional mode)
        self.partition_epoch: int = 0

        # Counters for the dashboard
        self.total_messages_sent: int = 0
        self.total_messages_received: int = 0
        self.leader_changes: int = 0

        self._running = False

    # ================================================================ timers

    @staticmethod
    def _new_election_deadline() -> float:
        timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        return time.monotonic() + timeout

    def _reset_election_timer(self) -> None:
        self._election_deadline = self._new_election_deadline()

    # ============================================================ log helpers

    @property
    def last_log_index(self) -> int:
        return len(self.log)

    @property
    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def _log_is_up_to_date(self, last_index: int, last_term: int) -> bool:
        """Raft §5.4.1 — at-least-as-up-to-date check."""
        if last_term != self.last_log_term:
            return last_term > self.last_log_term
        return last_index >= self.last_log_index

    # ============================================================= main loop

    async def run(self) -> None:
        """Start the coordinator event loop."""
        self._running = True
        self.transport.set_handler(self._dispatch)
        logger.info(
            "%s starting as %s (battery=%d%%)",
            self.node_id, self.state.value, self.battery.read_level(),
        )

        while self._running:
            self._check_battery()
            self._tick()
            self._publish_status()
            await asyncio.sleep(0.010)

    def stop(self) -> None:
        self._running = False

    # ======================================================= battery gating

    def _check_battery(self) -> None:
        level = self.battery.read_level()
        if level < T_LOW and self.state != NodeState.OBSERVER:
            logger.info(
                "%s battery %d%% < T_LOW — becoming OBSERVER",
                self.node_id, level,
            )
            self.state = NodeState.OBSERVER
            self.voted_for = None
        elif level >= T_RESTORE and self.state == NodeState.OBSERVER:
            logger.info(
                "%s battery %d%% ≥ T_RESTORE — re-entering as FOLLOWER",
                self.node_id, level,
            )
            self.state = NodeState.FOLLOWER

    # ============================================================= tick loop

    def _tick(self) -> None:
        now = time.monotonic()

        if self.state == NodeState.OBSERVER:
            return

        if self.state in (NodeState.LEADER, NodeState.LOCAL_LEADER):
            if now - self._last_ping_time >= LIVENESS_PING_INTERVAL:
                self._send_liveness_ping()
                self._last_ping_time = now
            return

        # Follower / candidate — check election timeout
        if now >= self._election_deadline:
            self._start_election()

    # ============================================================= election

    def _start_election(self) -> None:
        if self.state == NodeState.OBSERVER:
            return

        self.current_term += 1
        self.state = NodeState.CANDIDATE
        self.voted_for = self.node_id
        battery_level = self.battery.read_level()
        self._votes = {self.node_id: battery_level}
        self._reset_election_timer()

        logger.info(
            "%s starting election for term %d (battery=%d%%)",
            self.node_id, self.current_term, battery_level,
        )

        for peer in self.peers:
            self.transport.send(peer, "request_vote", {
                "term": self.current_term,
                "candidate_id": self.node_id,
                "last_log_index": self.last_log_index,
                "last_log_term": self.last_log_term,
                "battery_level": battery_level,
            })
            self.total_messages_sent += 1

    # ======================================================== msg dispatch

    def _dispatch(self, sender: str, msg_type: str, payload: dict) -> None:
        """Called from the MQTT thread for every inbound message."""
        self.total_messages_received += 1

        _HANDLERS = {
            "request_vote": self._handle_request_vote,
            "vote_response": self._handle_vote_response,
            "append_entries": self._handle_append_entries,
            "append_response": self._handle_append_entries_response,
            "liveness_ping": self._handle_liveness_ping,
            "leaf_register": self._handle_leaf_register,
            "sensor_data": self._handle_sensor_data,
            "demo_control": self._handle_demo_control,
        }
        handler = _HANDLERS.get(msg_type)
        if handler:
            handler(sender, payload)
        else:
            logger.warning("%s received unknown msg_type=%s", self.node_id, msg_type)

    # ======================================================== RequestVote

    def _handle_request_vote(self, sender: str, p: dict) -> None:
        term = p["term"]
        candidate_id = p["candidate_id"]
        battery_level = p["battery_level"]

        if term > self.current_term:
            self._step_down(term)

        grant = (
            term == self.current_term
            and self.state != NodeState.OBSERVER
            and battery_level >= T_LOW
            and (self.voted_for is None or self.voted_for == candidate_id)
            and self._log_is_up_to_date(p["last_log_index"], p["last_log_term"])
        )

        if grant:
            self.voted_for = candidate_id
            self._reset_election_timer()

        self.transport.send(sender, "vote_response", {
            "term": self.current_term,
            "vote_granted": grant,
            "voter_id": self.node_id,
            "voter_battery": self.battery.read_level(),
        })
        self.total_messages_sent += 1

    def _handle_vote_response(self, sender: str, p: dict) -> None:
        if p["term"] > self.current_term:
            self._step_down(p["term"])
            return

        if self.state != NodeState.CANDIDATE or p["term"] != self.current_term:
            return

        if p["vote_granted"]:
            self._votes[sender] = p["voter_battery"]

        # Majority among all coordinators (self + peers)
        if len(self._votes) > (len(self.peers) + 1) // 2:
            self._become_leader()

    # ------------------------------------------------------------- helpers

    def _become_leader(self) -> None:
        self.state = NodeState.LEADER
        self.leader_changes += 1
        logger.info(
            "%s became LEADER for term %d",
            self.node_id, self.current_term,
        )
        for peer in self.peers:
            self.next_index[peer] = self.last_log_index + 1
            self.match_index[peer] = 0
        self._last_ping_time = time.monotonic()
        self._send_liveness_ping()

    def _step_down(self, new_term: int) -> None:
        self.current_term = new_term
        self.voted_for = None
        if self.state in (
            NodeState.CANDIDATE, NodeState.LEADER, NodeState.LOCAL_LEADER,
        ):
            self.state = NodeState.FOLLOWER

    # ====================================================== AppendEntries

    def _handle_append_entries(self, sender: str, p: dict) -> None:
        term = p["term"]
        if term > self.current_term:
            self._step_down(term)

        if term < self.current_term:
            self.transport.send(sender, "append_response", {
                "term": self.current_term,
                "success": False,
                "responder_id": self.node_id,
                "match_index": 0,
            })
            self.total_messages_sent += 1
            return

        self._reset_election_timer()
        if self.state == NodeState.CANDIDATE:
            self.state = NodeState.FOLLOWER

        prev_log_index = p["prev_log_index"]
        prev_log_term = p["prev_log_term"]

        if prev_log_index > 0:
            if prev_log_index > len(self.log):
                self._reject_append(sender)
                return
            if self.log[prev_log_index - 1].term != prev_log_term:
                self._reject_append(sender)
                return

        for raw in p.get("entries", []):
            entry = LogEntry.from_dict(raw)
            if entry.index <= len(self.log):
                if self.log[entry.index - 1].term != entry.term:
                    self.log = self.log[: entry.index - 1]
                    self.log.append(entry)
            else:
                self.log.append(entry)

        leader_commit = p.get("leader_commit", 0)
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log))

        self.transport.send(sender, "append_response", {
            "term": self.current_term,
            "success": True,
            "responder_id": self.node_id,
            "match_index": len(self.log),
        })
        self.total_messages_sent += 1

    def _reject_append(self, sender: str) -> None:
        self.transport.send(sender, "append_response", {
            "term": self.current_term,
            "success": False,
            "responder_id": self.node_id,
            "match_index": 0,
        })
        self.total_messages_sent += 1

    def _handle_append_entries_response(self, sender: str, p: dict) -> None:
        if p["term"] > self.current_term:
            self._step_down(p["term"])
            return

        if self.state != NodeState.LEADER:
            return

        if p["success"]:
            self.next_index[sender] = p["match_index"] + 1
            self.match_index[sender] = p["match_index"]
            self._try_advance_commit()
        else:
            self.next_index[sender] = max(1, self.next_index.get(sender, 1) - 1)

    def _try_advance_commit(self) -> None:
        for n in range(len(self.log), self.commit_index, -1):
            if self.log[n - 1].term != self.current_term:
                continue
            repl = 1  # count self
            for peer in self.peers:
                if self.match_index.get(peer, 0) >= n:
                    repl += 1
            if repl > (len(self.peers) + 1) // 2:
                self.commit_index = n
                logger.info("%s committed up to index %d", self.node_id, n)
                break

    # ========================================================= replication

    def _replicate_entry(
        self, command: Any, trigger: str = "delta",
    ) -> LogEntry:
        entry = LogEntry(
            index=self.last_log_index + 1,
            term=self.current_term,
            command=command,
            trigger_type=trigger,
            partition_epoch=self.partition_epoch,
        )
        self.log.append(entry)
        for peer in self.peers:
            self._send_append_entries(peer)
        return entry

    def _send_append_entries(self, peer_id: str) -> None:
        prev_idx = self.next_index.get(peer_id, 1) - 1
        prev_term = (
            self.log[prev_idx - 1].term if 0 < prev_idx <= len(self.log) else 0
        )
        entries = [e.to_dict() for e in self.log[prev_idx:]]

        self.transport.send(peer_id, "append_entries", {
            "term": self.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_idx,
            "prev_log_term": prev_term,
            "entries": entries,
            "leader_commit": self.commit_index,
            "trigger_type": "delta",
            "partition_epoch": self.partition_epoch,
        })
        self.total_messages_sent += 1

    # ========================================================= liveness

    def _send_liveness_ping(self) -> None:
        self.transport.broadcast("liveness_ping", {
            "term": self.current_term,
            "leader_id": self.node_id,
        })
        self.total_messages_sent += 1

    def _handle_liveness_ping(self, sender: str, p: dict) -> None:
        term = p["term"]
        if term >= self.current_term:
            if term > self.current_term:
                self._step_down(term)
            self._reset_election_timer()

    # ====================================================== leaf management

    def _handle_leaf_register(self, sender: str, p: dict) -> None:
        leaf_id = p["leaf_id"]
        self._leaves.add(leaf_id)
        logger.info("%s registered leaf %s", self.node_id, leaf_id)
        self.transport.send(sender, "leaf_register_response", {
            "accepted": True,
            "coordinator_id": self.node_id,
            "term": self.current_term,
        })
        self.total_messages_sent += 1

    def _handle_sensor_data(self, sender: str, p: dict) -> None:
        if self.state != NodeState.LEADER:
            return

        leaf_id = p["leaf_id"]
        sensor_type = p["sensor_type"]
        value = p["value"]
        key = f"{leaf_id}:{sensor_type}"
        prev = self._last_sensor.get(key)

        if prev is not None:
            if abs(value - prev) / max(abs(prev), 1e-9) < DELTA_THRESHOLD:
                return

        self._last_sensor[key] = value
        self._replicate_entry(
            command={"sensor": sensor_type, "value": value, "leaf": leaf_id},
            trigger="delta",
        )

    # ============================================================ demo (MQTT)

    def _handle_demo_control(self, sender: str, p: dict) -> None:
        """Apply dashboard demo commands (mock battery only). Broadcast on MQTT."""
        if os.getenv("ECHO_DEMO", "1") != "1":
            return
        if not self.battery.is_mock:
            return
        action = p.get("action")
        if action == "set_battery":
            level = p.get("level", 100)
            self.battery.set_mock_level(float(level))
            logger.info("%s demo: mock battery set to %s%%", self.node_id, level)
        elif action == "set_drain_rate":
            rate = p.get("rate", 0.5)
            self.battery.set_mock_drain_rate(float(rate))
            logger.info("%s demo: mock drain rate set to %s", self.node_id, rate)
        elif action == "set_drain_paused":
            paused = bool(p.get("paused", False))
            self.battery.set_mock_drain_paused(paused)
            logger.info("%s demo: mock drain paused=%s", self.node_id, paused)
        else:
            logger.warning("%s demo: unknown action %r", self.node_id, action)
            return
        self._check_battery()
        self._publish_status()

    # ======================================================= dashboard status

    def _publish_status(self) -> None:
        self.transport.publish_status({
            "state": self.state.value,
            "term": self.current_term,
            "battery": self.battery.read_level(),
            "log_length": len(self.log),
            "commit_index": self.commit_index,
            "voted_for": self.voted_for,
            "peers": self.peers,
            "leaves": list(self._leaves),
            "messages_sent": self.total_messages_sent,
            "messages_received": self.total_messages_received,
            "leader_changes": self.leader_changes,
        })

    # ====================================================== provisional mode

    def enter_provisional_mode(self) -> None:
        self.partition_epoch += 1
        if self.state == NodeState.LEADER:
            self.state = NodeState.LOCAL_LEADER
        logger.info(
            "%s entering provisional mode (epoch=%d)",
            self.node_id, self.partition_epoch,
        )

    def exit_provisional_mode(self) -> None:
        if self.state == NodeState.LOCAL_LEADER:
            self.state = NodeState.FOLLOWER
        self.partition_epoch = 0
        logger.info("%s exited provisional mode", self.node_id)


# ======================================================================= CLI

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ECHO coordinator node (RPi / mock)")
    p.add_argument("--node-id", required=True, help="Unique node identifier")
    p.add_argument(
        "--peers", required=True,
        help="Comma-separated peer node IDs (e.g. coord-1,coord-2)",
    )
    p.add_argument("--broker", default=BROKER_HOST, help="MQTT broker host")
    p.add_argument("--port", type=int, default=BROKER_PORT, help="MQTT broker port")
    p.add_argument("--cluster", default=CLUSTER_ID, help="Cluster ID")
    p.add_argument(
        "--mock", action="store_true",
        help="Use mock battery (simulated drain) instead of real GPIO",
    )
    p.add_argument(
        "--battery", type=float, default=100.0,
        help="Initial mock battery level (default: 100)",
    )
    return p.parse_args(argv)


async def main(args: argparse.Namespace) -> None:
    peers = [p.strip() for p in args.peers.split(",") if p.strip()]

    transport = MQTTTransport(
        node_id=args.node_id,
        cluster_id=args.cluster,
        broker_host=args.broker,
        broker_port=args.port,
    )
    battery = BatteryMonitor(mock=args.mock, initial_level=args.battery)
    node = EchoCoordinator(
        node_id=args.node_id,
        peers=peers,
        transport=transport,
        battery=battery,
    )

    transport.connect()
    try:
        await node.run()
    finally:
        transport.disconnect()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    args = parse_args()
    asyncio.run(main(args))
