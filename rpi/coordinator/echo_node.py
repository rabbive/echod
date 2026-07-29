"""Consensus coordinator node for Raspberry Pi deployment.

Runs one of three protocols over MQTT transport with real (or mock)
battery monitoring, selected with ``--protocol``:

- ``raft``  — flat Raft baseline: full AppendEntries heartbeats to all
  peers, every sensor event is its own consensus round, no energy gating.
- ``echo``  — ECHO: tiered, energy-gated elections, broadcast liveness
  pings, coordinator-side delta filtering (default).
- ``echod`` — echoD hybrid: ECHO's architecture plus six optimizations
  (edge filtering, batched consensus, coordinators-only adaptive pings,
  battery-ordered election timeouts, directed leader handoff, batched
  reconciliation).  Mirrors ``simulation/protocols/echod.py``.

All three share the same transport, battery monitor, log machinery, and
workload path so hardware comparisons are honest by construction.  Can
be launched as a standalone process — one per Raspberry Pi in the
physical cluster, or several on a single laptop for a hardware-free demo.

Usage::

    python -m rpi.coordinator.echo_node \
        --node-id coord-0 \
        --peers coord-1,coord-2,coord-3,coord-4 \
        --protocol echod \
        --mock
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import time
import zlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from rpi.config import (
    BATCH_WINDOW_S,
    BROKER_HOST,
    BROKER_PORT,
    CLUSTER_ID,
    DELTA_THRESHOLD,
    ECHOD_ELECTION_TIMEOUT_MAX,
    ECHOD_ELECTION_TIMEOUT_MIN,
    ECHOD_PING_MAX_INTERVAL,
    ELECTION_TIE_BREAK_S,
    ELECTION_TIMEOUT_MAX,
    ELECTION_TIMEOUT_MIN,
    LEAF_KEEPALIVE_INTERVAL,
    LIVENESS_BACKOFF_FACTOR,
    LIVENESS_PING_INTERVAL,
    MAX_BATCH_SIZE,
    RAFT_HEARTBEAT_INTERVAL,
    T_HANDOFF,
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
    """Consensus coordinator running over MQTT.

    Despite the historical class name, this node runs Raft, ECHO, or
    echoD depending on the ``protocol`` parameter.  The coordinator
    participates in leader election, log replication, and liveness
    management.  In ECHO/echoD modes, energy-aware vote gating rejects
    candidates whose battery has dropped below ``T_LOW``.
    """

    PROTOCOLS = ("raft", "echo", "echod")

    def __init__(
        self,
        node_id: str,
        peers: list[str],
        transport: MQTTTransport,
        battery: BatteryMonitor,
        protocol: str = "echo",
    ) -> None:
        if protocol not in self.PROTOCOLS:
            raise ValueError(
                f"unknown protocol {protocol!r} — expected one of {self.PROTOCOLS}"
            )
        self.protocol = protocol
        self.node_id = node_id
        # Peers must exclude ourselves; otherwise quorum math and replication
        # fanout are incorrect in multi-coordinator deployments (including the
        # local hardware-free demo which passes a full peer list).
        self.peers = [p for p in peers if p != node_id]
        self.transport = transport
        self.battery = battery

        # Deterministic per-node tie-break for echoD battery-ordered
        # timeouts (crc32 is stable across runs, unlike hash()).  Must be
        # set BEFORE the initial _new_election_deadline() call below.
        self._tie_break_s = (
            zlib.crc32(node_id.encode()) % int(ELECTION_TIE_BREAK_S * 1000)
        ) / 1000.0

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

        # Leader tracking + triggers buffered while leaderless.  Followers
        # forward sensor reports to the known leader; if none is known the
        # report is buffered and replayed when this node becomes leader.
        self._current_leader_id: str | None = None
        self._pending_triggers: list[tuple[Any, str]] = []

        # Partition epoch (non-zero in provisional mode)
        self.partition_epoch: int = 0

        # ---- echoD-only state (unused in raft/echo modes) ----
        # Batching (optimization 2)
        self._batch_buffer: list[Any] = []
        self._batch_deadline: float | None = None
        # Adaptive liveness (optimization 3)
        self._ping_interval_s: float = LIVENESS_PING_INTERVAL
        self._last_consensus_activity: float = time.monotonic()
        self._last_leaf_keepalive: float = 0.0
        # Handoff (optimization 5): peer batteries learned from
        # append_response messages; _handoff_sent arms once per term.
        self._peer_batteries: dict[str, float] = {}
        self._handoff_sent: bool = False

        # Counters for the dashboard / experiment harness.  Per-type
        # counters mirror the simulation collector: RX-side counts match
        # the sim's delivery-based accounting (a broadcast ping is counted
        # once per node that receives it).
        self.total_messages_sent: int = 0
        self.total_messages_received: int = 0
        self.messages_sent_by_type: dict[str, int] = {}
        self.messages_recv_by_type: dict[str, int] = {}
        self.leader_changes: int = 0

        # Commit-latency tracking (leader-side arrival → commit; uses the
        # local monotonic clock so no cross-node clock sync is needed).
        self._pending_latency: dict[int, float] = {}
        self._latencies: list[float] = []

        # Partition test hook: inbound messages from these senders are
        # dropped (mirrors the simulation's message-bus partition).
        self._blocked_senders: set[str] = set()
        # Idempotent reconciliation: every node on the losing side of a
        # partition forwards its truncated provisional commands, so the
        # same command can arrive several times — replay each once.
        self._recent_reconciles: list[str] = []

        self._running = False
        # Messages arrive on the MQTT client's background thread. We enqueue
        # them into the asyncio loop and handle them in-order on the coordinator
        # task to avoid racy state mutations.
        self._loop: asyncio.AbstractEventLoop | None = None
        self._inbox: asyncio.Queue[tuple[str, str, dict]] | None = None

    # ==================================================== counted messaging

    def _send(self, recipient: str, msg_type: str, payload: dict) -> None:
        """Directed send with harness accounting (TX side)."""
        self.transport.send(recipient, msg_type, payload)
        self.total_messages_sent += 1
        self.messages_sent_by_type[msg_type] = (
            self.messages_sent_by_type.get(msg_type, 0) + 1
        )

    def _broadcast(self, msg_type: str, payload: dict) -> None:
        """Broadcast send with harness accounting (TX side)."""
        self.transport.broadcast(msg_type, payload)
        self.total_messages_sent += 1
        self.messages_sent_by_type[msg_type] = (
            self.messages_sent_by_type.get(msg_type, 0) + 1
        )

    # ================================================================ timers

    def _new_election_deadline(self) -> float:
        """Next election deadline.

        raft/echo: uniform random timeout (Raft §5.2).
        echod: battery-ordered timeout (optimization 4) — the highest-
        battery coordinator always times out first and wins on the first
        ballot; ties are broken by a stable per-node offset.
        """
        if self.protocol == "echod":
            spread = ECHOD_ELECTION_TIMEOUT_MAX - ECHOD_ELECTION_TIMEOUT_MIN
            timeout = (
                ECHOD_ELECTION_TIMEOUT_MIN
                + (1.0 - self.battery.read_level() / 100.0) * spread
                + self._tie_break_s
            )
        else:
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
        self._loop = asyncio.get_running_loop()
        self._inbox = asyncio.Queue()
        self.transport.set_handler(self._dispatch)
        logger.info(
            "%s starting as %s (battery=%d%%)",
            self.node_id, self.state.value, self.battery.read_level(),
        )

        logger.info("%s running protocol=%s", self.node_id, self.protocol)
        while self._running:
            self._drain_inbox()
            self._check_battery()
            self._tick()
            self._publish_status()
            await asyncio.sleep(0.005)

    def stop(self) -> None:
        self._running = False

    # ======================================================= battery gating

    def _check_battery(self) -> None:
        # Raft has no energy gating — every node participates regardless
        # of battery level (the battery is still monitored for metrics).
        if self.protocol == "raft":
            return
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
            if self.protocol == "raft":
                # Full AppendEntries heartbeats to all peers at a fixed
                # interval — Raft's idle cost.
                if now - self._last_ping_time >= RAFT_HEARTBEAT_INTERVAL:
                    self._send_heartbeats()
                    self._last_ping_time = now
            elif self.protocol == "echod":
                # Flush the batch when its window closes (optimization 2)
                if self._batch_deadline is not None and now >= self._batch_deadline:
                    self._flush_batch()
                # Adaptive coordinators-only ping (optimization 3)
                if now - self._last_ping_time >= self._ping_interval_s:
                    self._send_liveness_ping()
                    self._last_ping_time = now
                # Directed handoff when battery runs low (optimization 5)
                if (
                    self.battery.read_level() < T_HANDOFF
                    and not self._handoff_sent
                ):
                    self._initiate_handoff()
            else:  # echo
                if now - self._last_ping_time >= LIVENESS_PING_INTERVAL:
                    self._send_liveness_ping()
                    self._last_ping_time = now
        # Follower / candidate — check election timeout
        elif now >= self._election_deadline:
            self._start_election()

        # echoD: every coordinator keepalives *its own* leaves at a slow
        # fixed rate (optimization 3), so leaves registered to a non-leader
        # no longer flap between ACTIVE and SEARCHING.
        if (
            self.protocol == "echod"
            and self._leaves
            and now - self._last_leaf_keepalive >= LEAF_KEEPALIVE_INTERVAL
        ):
            self._send_leaf_keepalives()
            self._last_leaf_keepalive = now

    # ============================================================= election

    def _start_election(self) -> None:
        if self.state == NodeState.OBSERVER:
            return

        self.current_term += 1
        self.state = NodeState.CANDIDATE
        self.voted_for = self.node_id
        self._current_leader_id = None
        battery_level = self.battery.read_level()
        self._votes = {self.node_id: battery_level}
        self._reset_election_timer()

        logger.info(
            "%s starting election for term %d (battery=%d%%)",
            self.node_id, self.current_term, battery_level,
        )

        for peer in self.peers:
            self._send(peer, "request_vote", {
                "term": self.current_term,
                "candidate_id": self.node_id,
                "last_log_index": self.last_log_index,
                "last_log_term": self.last_log_term,
                "battery_level": battery_level,
            })

    # ======================================================== msg dispatch

    def _dispatch(self, sender: str, msg_type: str, payload: dict) -> None:
        """Called from the MQTT thread for every inbound message."""
        # Partition test hook: act as if the network dropped the packet.
        # partition_control itself is never blocked (it must reach both
        # sides so a heal can be delivered).
        if (
            msg_type != "partition_control"
            and sender in self._blocked_senders
        ):
            return
        loop = self._loop
        inbox = self._inbox
        if loop is None or inbox is None:
            return
        loop.call_soon_threadsafe(inbox.put_nowait, (sender, msg_type, payload))

    def _drain_inbox(self) -> None:
        """Process all queued inbound MQTT messages (asyncio task thread)."""
        inbox = self._inbox
        if inbox is None:
            return
        while True:
            try:
                sender, msg_type, payload = inbox.get_nowait()
            except asyncio.QueueEmpty:
                return

            self.total_messages_received += 1
            self.messages_recv_by_type[msg_type] = (
                self.messages_recv_by_type.get(msg_type, 0) + 1
            )

            _HANDLERS = {
                "request_vote": self._handle_request_vote,
                "vote_response": self._handle_vote_response,
                "append_entries": self._handle_append_entries,
                "append_response": self._handle_append_entries_response,
                "liveness_ping": self._handle_liveness_ping,
                "leaf_register": self._handle_leaf_register,
                "sensor_data": self._handle_sensor_data,
                "leadership_handoff": self._handle_leadership_handoff,
                "reconcile_batch": self._handle_reconcile_batch,
                "partition_control": self._handle_partition_control,
                "demo_control": self._handle_demo_control,
            }
            handler = _HANDLERS.get(msg_type)
            if handler:
                handler(sender, payload)
            else:
                logger.warning(
                    "%s received unknown msg_type=%s", self.node_id, msg_type
                )

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
            # Energy gating is an ECHO/echoD feature; Raft votes regardless.
            and (self.protocol == "raft" or battery_level >= T_LOW)
            and (self.voted_for is None or self.voted_for == candidate_id)
            and self._log_is_up_to_date(p["last_log_index"], p["last_log_term"])
        )

        if grant:
            self.voted_for = candidate_id
            self._reset_election_timer()

        self._send(sender, "vote_response", {
            "term": self.current_term,
            "vote_granted": grant,
            "voter_id": self.node_id,
            "voter_battery": self.battery.read_level(),
        })

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
        self._current_leader_id = self.node_id
        logger.info(
            "%s became LEADER for term %d",
            self.node_id, self.current_term,
        )
        for peer in self.peers:
            self.next_index[peer] = self.last_log_index + 1
            self.match_index[peer] = 0
        # Reset echoD per-term state (optimizations 3 + 5)
        self._handoff_sent = False
        self._ping_interval_s = LIVENESS_PING_INTERVAL
        self._last_consensus_activity = time.monotonic()
        self._last_ping_time = time.monotonic()
        # Assert leadership immediately (Raft heartbeats double as this).
        if self.protocol == "raft":
            self._send_heartbeats()
        else:
            self._send_liveness_ping()
        # Replay triggers buffered while no leader was known.
        self._flush_pending_triggers()

    def _step_down(self, new_term: int) -> None:
        self.current_term = new_term
        self.voted_for = None
        if self.state in (
            NodeState.CANDIDATE, NodeState.LEADER, NodeState.LOCAL_LEADER,
        ):
            self.state = NodeState.FOLLOWER
        # Avoid immediate re-election churn after learning about a newer term.
        # In the MQTT demo environment, scheduling jitter can otherwise cause
        # followers to time out immediately and start a new election.
        if self.state != NodeState.OBSERVER:
            self._reset_election_timer()

    # ====================================================== AppendEntries

    def _handle_append_entries(self, sender: str, p: dict) -> None:
        term = p["term"]
        if term > self.current_term:
            self._step_down(term)

        if term < self.current_term:
            self._send(sender, "append_response", {
                "term": self.current_term,
                "success": False,
                "responder_id": self.node_id,
                "match_index": 0,
                "responder_battery": self.battery.read_level(),
            })
            return

        self._reset_election_timer()
        self._current_leader_id = p.get("leader_id", sender)
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
                    # Conflict — truncate.  Provisional entries from the
                    # losing side of a partition are replayed to the new
                    # leader (batched reconciliation, echoD optimization 6;
                    # in echo mode the leader replays one round per entry).
                    truncated = self.log[entry.index - 1 :]
                    self.log = self.log[: entry.index - 1]
                    self.log.append(entry)
                    self._replay_provisional(truncated)
            else:
                self.log.append(entry)

        leader_commit = p.get("leader_commit", 0)
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log))

        self._send(sender, "append_response", {
            "term": self.current_term,
            "success": True,
            "responder_id": self.node_id,
            "match_index": len(self.log),
            "responder_battery": self.battery.read_level(),
        })

    def _reject_append(self, sender: str) -> None:
        self._send(sender, "append_response", {
            "term": self.current_term,
            "success": False,
            "responder_id": self.node_id,
            "match_index": 0,
            "responder_battery": self.battery.read_level(),
        })

    def _replay_provisional(self, truncated: list[LogEntry]) -> None:
        """Forward truncated provisional commands to the new leader.

        One ``reconcile_batch`` message carries every truncated command —
        the leader decides the replay cost (echo: one round per entry;
        echoD: a single batch round).
        """
        commands = [e.command for e in truncated if e.partition_epoch != 0]
        if not commands or self.protocol == "raft":
            return
        leader_id = self._current_leader_id
        if leader_id is None or leader_id == self.node_id:
            return
        logger.info(
            "%s replaying %d provisional entr%s to leader %s",
            self.node_id, len(commands),
            "y" if len(commands) == 1 else "ies", leader_id,
        )
        self._send(leader_id, "reconcile_batch", {
            "term": self.current_term,
            "sender_node": self.node_id,
            "commands": commands,
        })

    def _handle_append_entries_response(self, sender: str, p: dict) -> None:
        if p["term"] > self.current_term:
            self._step_down(p["term"])
            return

        # Peer battery telemetry feeds echoD's handoff candidate choice
        # (optimization 5); tracked in all modes for the dashboard.
        if "responder_battery" in p:
            self._peer_batteries[sender] = float(p["responder_battery"])

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
                now = time.monotonic()
                for idx in sorted(self._pending_latency):
                    if idx <= n:
                        self._latencies.append(
                            now - self._pending_latency.pop(idx)
                        )
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
        self._pending_latency[entry.index] = time.monotonic()
        for peer in self.peers:
            self._send_append_entries(peer)
        return entry

    def _send_append_entries(self, peer_id: str) -> None:
        prev_idx = self.next_index.get(peer_id, 1) - 1
        prev_term = (
            self.log[prev_idx - 1].term if 0 < prev_idx <= len(self.log) else 0
        )
        entries = [e.to_dict() for e in self.log[prev_idx:]]

        self._send(peer_id, "append_entries", {
            "term": self.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_idx,
            "prev_log_term": prev_term,
            "entries": entries,
            "leader_commit": self.commit_index,
            "trigger_type": "delta",
            "partition_epoch": self.partition_epoch,
        })

    # ========================================================= liveness

    def _send_heartbeats(self) -> None:
        """Raft mode: full AppendEntries heartbeats to every peer."""
        for peer in self.peers:
            self._send_append_entries(peer)

    def _send_liveness_ping(self) -> None:
        """ECHO: one broadcast ping to the whole cluster (leaves included).

        echoD: directed pings to coordinators only (optimization 3), with
        exponential backoff while the cluster is idle and snap-back the
        moment consensus is active.
        """
        if self.protocol == "echod":
            for peer in self.peers:
                self._send(peer, "liveness_ping", {
                    "term": self.current_term,
                    "leader_id": self.node_id,
                })
            if self._last_consensus_activity >= self._last_ping_time:
                self._ping_interval_s = LIVENESS_PING_INTERVAL
            else:
                self._ping_interval_s = min(
                    self._ping_interval_s * LIVENESS_BACKOFF_FACTOR,
                    ECHOD_PING_MAX_INTERVAL,
                )
        else:
            self._broadcast("liveness_ping", {
                "term": self.current_term,
                "leader_id": self.node_id,
            })

    def _send_leaf_keepalives(self) -> None:
        """echoD: slow liveness ping to this coordinator's own leaves."""
        for leaf_id in list(self._leaves):
            self._send(leaf_id, "liveness_ping", {
                "term": self.current_term,
                "leader_id": self.node_id,
            })

    def _handle_liveness_ping(self, sender: str, p: dict) -> None:
        term = p["term"]
        if term >= self.current_term:
            if term > self.current_term:
                self._step_down(term)
            self._current_leader_id = p.get("leader_id", sender)
            self._reset_election_timer()

    # ====================================================== leaf management

    def _handle_leaf_register(self, sender: str, p: dict) -> None:
        leaf_id = p["leaf_id"]
        self._leaves.add(leaf_id)
        logger.info("%s registered leaf %s", self.node_id, leaf_id)
        self._send(sender, "leaf_register_response", {
            "accepted": True,
            "coordinator_id": self.node_id,
            "term": self.current_term,
        })

    def _handle_sensor_data(self, sender: str, p: dict) -> None:
        command = {
            "sensor": p["sensor_type"],
            "value": p["value"],
            "leaf": p["leaf_id"],
        }

        if self.state not in (NodeState.LEADER, NodeState.LOCAL_LEADER):
            # Forward to the known leader; buffer if none is known and
            # replay on promotion (supporting fix for a fair comparison —
            # sensor reports are never silently dropped).
            if (
                self._current_leader_id is not None
                and self._current_leader_id != self.node_id
            ):
                self._send(self._current_leader_id, "sensor_data", p)
            else:
                self._pending_triggers.append((command, "delta"))
            return

        if self.protocol == "raft":
            # Raft has no filtering — every report is its own round.
            self._replicate_entry(command=command, trigger="event")
            return

        leaf_id = p["leaf_id"]
        sensor_type = p["sensor_type"]
        value = p["value"]
        key = f"{leaf_id}:{sensor_type}"
        prev = self._last_sensor.get(key)

        # Coordinator-side delta check: the only filter in ECHO, a
        # backstop in echoD (where leaves normally pre-filter).
        if prev is not None:
            if abs(value - prev) / max(abs(prev), 1e-9) < DELTA_THRESHOLD:
                return
        self._last_sensor[key] = value

        if self.protocol == "echod":
            # Batched event-driven consensus (optimization 2): buffer into
            # the current window; a full batch flushes immediately so there
            # is no added latency under load.
            self._batch_buffer.append(command)
            if self._batch_deadline is None:
                self._batch_deadline = time.monotonic() + BATCH_WINDOW_S
            if len(self._batch_buffer) >= MAX_BATCH_SIZE:
                self._flush_batch()
        else:  # echo — one consensus round per event
            self._replicate_entry(command=command, trigger="delta")

    def _flush_batch(self) -> None:
        """echoD: replicate all buffered readings as a single log entry."""
        if not self._batch_buffer:
            self._batch_deadline = None
            return
        commands = list(self._batch_buffer)
        self._batch_buffer.clear()
        self._batch_deadline = None
        command: Any = (
            commands[0]
            if len(commands) == 1
            else {"batch": commands, "count": len(commands)}
        )
        self._last_consensus_activity = time.monotonic()
        self._replicate_entry(command=command, trigger="delta")

    def _flush_pending_triggers(self) -> None:
        """Replay triggers buffered while leaderless.

        raft/echo: one consensus round per trigger.  echoD: the whole
        buffer replays as ONE batch entry (also used on reconciliation —
        optimization 6).
        """
        if not self._pending_triggers:
            return
        pending = list(self._pending_triggers)
        self._pending_triggers.clear()
        self._last_consensus_activity = time.monotonic()
        if self.protocol == "echod" and len(pending) > 1:
            self._replicate_entry(
                command={
                    "batch": [c for c, _ in pending],
                    "count": len(pending),
                },
                trigger=pending[0][1],
            )
            return
        for command, trigger in pending:
            self._replicate_entry(command=command, trigger=trigger)

    def _handle_reconcile_batch(self, sender: str, p: dict) -> None:
        """Leader: re-propose provisional commands from a healed partition.

        echo: one round per command (ECHO's per-entry reconciliation).
        echoD: all commands in ONE batch entry (optimization 6).
        """
        if self.state not in (NodeState.LEADER, NodeState.LOCAL_LEADER):
            return
        commands = p.get("commands", [])
        if not commands:
            return
        # Deduplicate: several losing-side nodes forward the same
        # provisional entries; each command is replayed only once.
        fresh: list[Any] = []
        for command in commands:
            key = json.dumps(command, sort_keys=True, default=str)
            if key not in self._recent_reconciles:
                self._recent_reconciles.append(key)
                fresh.append(command)
        if len(self._recent_reconciles) > 200:
            del self._recent_reconciles[:100]
        commands = fresh
        if not commands:
            logger.info(
                "%s ignoring duplicate reconcile from %s", self.node_id, sender,
            )
            return
        logger.info(
            "%s reconciling %d provisional command(s) from %s",
            self.node_id, len(commands), sender,
        )
        self._last_consensus_activity = time.monotonic()
        if self.protocol == "echod" and len(commands) > 1:
            self._replicate_entry(
                command={"batch": list(commands), "count": len(commands)},
                trigger="reconcile",
            )
        else:
            for command in commands:
                self._replicate_entry(command=command, trigger="reconcile")

    # ============================================ leader handoff (echoD)

    def _initiate_handoff(self) -> None:
        """echoD optimization 5: nominate the highest-battery successor.

        One directed message replaces a full randomized election round,
        and leadership moves without the election-timeout availability gap.
        """
        self._handoff_sent = True
        own_battery = self.battery.read_level()
        candidates = [
            (peer, self._peer_batteries.get(peer, 100.0))
            for peer in self.peers
        ]
        if not candidates:
            return
        # Highest battery first; node_id tie-break for determinism.
        candidates.sort(key=lambda nb: (-nb[1], nb[0]))
        successor, successor_battery = candidates[0]
        if successor_battery <= own_battery:
            return  # no better candidate — remain leader

        logger.info(
            "%s battery %d%% < T_HANDOFF — handing off to %s (%.0f%%)",
            self.node_id, own_battery, successor, successor_battery,
        )
        self._send(successor, "leadership_handoff", {
            "term": self.current_term,
            "leader_id": self.node_id,
        })
        self.state = NodeState.FOLLOWER
        self._reset_election_timer()

    def _handle_leadership_handoff(self, sender: str, p: dict) -> None:
        """Nominated successor: start an election immediately."""
        if self.protocol != "echod" or self.state == NodeState.OBSERVER:
            return
        term = p["term"]
        if term < self.current_term:
            return
        if term > self.current_term:
            self._step_down(term)
        logger.info(
            "%s accepted handoff nomination from %s",
            self.node_id, p.get("leader_id", sender),
        )
        self._start_election()

    # ==================================== partition test hook (harness)

    def _handle_partition_control(self, sender: str, p: dict) -> None:
        """Inject / heal a network partition (experiment harness only).

        ``{"action": "partition", "blocked_senders": [...]}`` drops all
        inbound traffic from the listed nodes — the mirror image of the
        simulation's message-bus partition.  ECHO/echoD additionally
        enter provisional mode; Raft does not (its minority side halts —
        that *is* the honest Raft behavior).

        ``{"action": "heal"}`` restores full connectivity and exits
        provisional mode; log reconciliation then happens through the
        normal AppendEntries conflict path (see ``_replay_provisional``).
        """
        action = p.get("action")
        if action == "partition":
            if "groups" in p:
                # One broadcast describes the full split; each node blocks
                # every group it is not a member of.
                blocked: set[str] = set()
                mine = None
                for group in p["groups"]:
                    if self.node_id in group:
                        mine = set(group)
                for group in p["groups"]:
                    if set(group) != mine:
                        blocked.update(group)
                if mine is not None:
                    blocked.discard(self.node_id)
                self._blocked_senders = blocked
            else:
                self._blocked_senders = set(p.get("blocked_senders", []))
            logger.warning(
                "%s PARTITIONED — blocking %d peer(s): %s",
                self.node_id, len(self._blocked_senders),
                sorted(self._blocked_senders),
            )
            if self.protocol != "raft":
                self.enter_provisional_mode()
        elif action == "heal":
            self._blocked_senders = set()
            logger.warning("%s partition HEALED", self.node_id)
            if self.protocol != "raft":
                self.exit_provisional_mode()
        else:
            logger.warning("%s unknown partition_control %r", self.node_id, p)

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
        avg_latency_ms = (
            (sum(self._latencies) / len(self._latencies)) * 1000.0
            if self._latencies else 0.0
        )
        self.transport.publish_status({
            "protocol": self.protocol,
            "state": self.state.value,
            "term": self.current_term,
            "battery": self.battery.read_level(),
            "log_length": len(self.log),
            "commit_index": self.commit_index,
            "voted_for": self.voted_for,
            "current_leader": self._current_leader_id,
            "partition_epoch": self.partition_epoch,
            "peers": self.peers,
            "leaves": list(self._leaves),
            "messages_sent": self.total_messages_sent,
            "messages_received": self.total_messages_received,
            "messages_sent_by_type": dict(self.messages_sent_by_type),
            "messages_recv_by_type": dict(self.messages_recv_by_type),
            "consensus_rounds": len(self._latencies),
            "avg_consensus_latency_ms": round(avg_latency_ms, 3),
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
    p = argparse.ArgumentParser(
        description="Consensus coordinator node — raft / echo / echod (RPi / mock)"
    )
    p.add_argument("--node-id", required=True, help="Unique node identifier")
    p.add_argument(
        "--peers", required=True,
        help="Comma-separated peer node IDs (e.g. coord-1,coord-2)",
    )
    p.add_argument(
        "--protocol", default="echo",
        choices=list(EchoCoordinator.PROTOCOLS),
        help="Consensus protocol to run (default: echo)",
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
        protocol=args.protocol,
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
