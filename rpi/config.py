"""Phase 2 configuration — shared constants for RPi coordinator and dashboard.

Mirrors simulation/core/config.py but adds MQTT broker and dashboard settings.
Timing values are in seconds (converted from the simulation's millisecond convention).
"""

from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# MQTT broker
# ---------------------------------------------------------------------------
BROKER_HOST: str = os.getenv("ECHO_BROKER_HOST", "localhost")
BROKER_PORT: int = int(os.getenv("ECHO_BROKER_PORT", "1883"))
CLUSTER_ID: str = os.getenv("ECHO_CLUSTER_ID", "echo-default")

# ---------------------------------------------------------------------------
# Timing (seconds)
# ---------------------------------------------------------------------------
#
# Note: Phase 2 runs over MQTT + Python threads/event-loop scheduling, which is
# far noisier than the in-process simulation. If election timeouts are too
# small, coordinators will spuriously start elections due to transient broker /
# scheduler jitter. Use larger defaults here for a stable live demo.
ELECTION_TIMEOUT_MIN: float = 1.2
ELECTION_TIMEOUT_MAX: float = 2.4
LIVENESS_PING_INTERVAL: float = 0.25
PARTITION_TIMEOUT: float = 2.0

# ---------------------------------------------------------------------------
# Energy thresholds (percentage 0-100)
# ---------------------------------------------------------------------------
T_LOW: int = 15
T_RESTORE: int = 25

# ---------------------------------------------------------------------------
# Consensus
# ---------------------------------------------------------------------------
DELTA_THRESHOLD: float = 0.05
MAX_COORDINATORS: int = 7
SAMPLE_INTERVAL: float = 0.100  # seconds between sensor polls

# ---------------------------------------------------------------------------
# Raft baseline (--protocol raft)
# ---------------------------------------------------------------------------
# Full AppendEntries heartbeats from the leader to every peer.
RAFT_HEARTBEAT_INTERVAL: float = 0.25

# ---------------------------------------------------------------------------
# echoD hybrid (--protocol echod) — mirrors simulation/core/config.py,
# scaled ~5x for MQTT + scheduler jitter (same as ELECTION_TIMEOUT_* above).
# ---------------------------------------------------------------------------
# Batched event-driven consensus: coalesce triggers inside one window into a
# single log entry.  The window is a local timer, so it keeps the sim value.
BATCH_WINDOW_S: float = 0.05
MAX_BATCH_SIZE: int = 10          # flush immediately when the buffer fills

# Adaptive liveness: leader pings coordinators only, backing off while idle.
ECHOD_PING_MAX_INTERVAL: float = 1.0   # cap; must stay < ECHOD_ELECTION_TIMEOUT_MIN
LIVENESS_BACKOFF_FACTOR: float = 2.0
LEAF_KEEPALIVE_INTERVAL: float = 5.0   # each coordinator -> its own leaves

# Battery-ordered election timeouts.  Longer range than raft/echo because the
# adaptive ping interval can grow to ECHOD_PING_MAX_INTERVAL.
ECHOD_ELECTION_TIMEOUT_MIN: float = 2.4
ECHOD_ELECTION_TIMEOUT_MAX: float = 4.8
ELECTION_TIE_BREAK_S: float = 0.15     # deterministic per-node spread (crc32)

# Leader handoff: below this battery % the leader nominates a successor
# directly (TimeoutNow-style) instead of dying into an election gap.
T_HANDOFF: int = 20

# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------
DASHBOARD_HOST: str = os.getenv("ECHO_DASHBOARD_HOST", "0.0.0.0")
DASHBOARD_PORT: int = int(os.getenv("ECHO_DASHBOARD_PORT", "5000"))

# ---------------------------------------------------------------------------
# Mock battery (used when --mock is passed)
# ---------------------------------------------------------------------------
# Keep the default drain low so the Phase 2 demo stays stable for a live
# presentation. You can still dial this up from the dashboard demo controls.
MOCK_BATTERY_DRAIN_RATE: float = 0.02  # % per second
