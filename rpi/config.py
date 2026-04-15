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
