"""ECHO protocol message types and RPC definitions.

Every protocol message is a frozen dataclass for immutability during routing.
Enums define the valid node states and consensus trigger types.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class NodeState(Enum):
    """Coordinator node states."""
    FOLLOWER = auto()
    CANDIDATE = auto()
    LEADER = auto()
    OBSERVER = auto()
    LOCAL_LEADER = auto()


class LeafState(Enum):
    """Leaf node states."""
    UNREGISTERED = auto()
    ACTIVE = auto()
    SEARCHING = auto()


class TriggerType(Enum):
    """Event types that trigger a consensus round in ECHO."""
    DELTA = auto()
    JOIN = auto()
    PARTITION = auto()
    RECONCILE = auto()
    LIVENESS = auto()


# ---------------------------------------------------------------------------
# Log entry
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LogEntry:
    """Single entry in the replicated log."""
    index: int
    term: int
    command: Any
    trigger_type: TriggerType
    partition_epoch: int = 0
    timestamp: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# RequestVote RPC
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RequestVoteRPC:
    """Sent by candidates to gather votes during an election.

    Extends Raft's RequestVote with battery_level for energy-weighted voting.
    The signature field carries an Ed25519 signature (simulated in Phase 1).
    """
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int
    battery_level: int  # 0-100
    signature: bytes = b""


@dataclass(frozen=True)
class RequestVoteResponse:
    """Reply to a RequestVoteRPC."""
    term: int
    vote_granted: bool
    voter_id: str
    voter_battery: int  # 0-100


# ---------------------------------------------------------------------------
# AppendEntries RPC
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AppendEntriesRPC:
    """Sent by the leader to replicate log entries and serve as heartbeat.

    In ECHO, trigger_type indicates *why* this round was initiated.
    partition_epoch is non-zero when operating in provisional consensus mode.
    """
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: tuple[LogEntry, ...] = ()
    leader_commit: int = 0
    trigger_type: TriggerType = TriggerType.LIVENESS
    partition_epoch: int = 0


@dataclass(frozen=True)
class AppendEntriesResponse:
    """Reply to an AppendEntriesRPC."""
    term: int
    success: bool
    responder_id: str
    match_index: int = 0


# ---------------------------------------------------------------------------
# Liveness ping (lightweight heartbeat replacement)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LivenessPing:
    """Lightweight ping sent between consensus rounds to confirm leader health.

    Designed to be ~32 bytes on the wire — carries no log data.
    """
    term: int
    leader_id: str
    timestamp: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Leaf ↔ Coordinator messages
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LeafRegisterRequest:
    """Sent by a leaf node to register with a coordinator."""
    leaf_id: str
    capabilities: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


@dataclass(frozen=True)
class LeafRegisterResponse:
    """Coordinator's reply to a leaf registration request."""
    accepted: bool
    coordinator_id: str
    term: int


@dataclass(frozen=True)
class SensorDataReport:
    """Sensor reading sent from a leaf to its coordinator."""
    leaf_id: str
    sensor_type: str
    value: float
    timestamp: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Generic message envelope
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Message:
    """Envelope that wraps any RPC or notification for the message bus.

    The bus uses sender_id / recipient_id to route; payload carries the
    actual RPC dataclass.
    """
    sender_id: str
    recipient_id: str
    payload: Any
    timestamp: float = field(default_factory=time.time)
