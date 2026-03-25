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
ELECTION_TIMEOUT_MIN: float = 0.150
ELECTION_TIMEOUT_MAX: float = 0.300
LIVENESS_PING_INTERVAL: float = 0.050
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
MOCK_BATTERY_DRAIN_RATE: float = 0.5  # % per second
