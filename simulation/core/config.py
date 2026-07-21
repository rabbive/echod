"""ECHO protocol configuration constants.

All timing, energy, consensus, and transport parameters live here.
Never hardcode these values elsewhere — always import from config.
"""

# Timing (milliseconds)
ELECTION_TIMEOUT_MIN = 150
ELECTION_TIMEOUT_MAX = 300
LIVENESS_PING_INTERVAL = 50
PARTITION_TIMEOUT = 2000

# Energy thresholds (percentage 0-100)
T_LOW = 15
T_RESTORE = 25

# Consensus
DELTA_THRESHOLD = 0.05
MAX_COORDINATORS = 7
SAMPLE_INTERVAL = 100  # ms, sensor polling rate

# Transport
MAX_RETRIES = 3
RETRY_BACKOFF_MS = 50
MESSAGE_SIGN = True

# ---------------------------------------------------------------------------
# echoD (Raft/ECHO hybrid) — constants below are echoD-specific
# ---------------------------------------------------------------------------

# Batched event-driven consensus: the leader coalesces triggers arriving
# within one window into a single log entry (one consensus round).
BATCH_WINDOW_MS = 50
MAX_BATCH_SIZE = 10          # flush immediately when the buffer reaches this

# Adaptive liveness: leader pings coordinators only, backing off while idle.
ECHOD_PING_MAX_INTERVAL = 250    # ms cap; must stay < ECHOD_ELECTION_TIMEOUT_MIN
LIVENESS_BACKOFF_FACTOR = 2.0
LEAF_KEEPALIVE_INTERVAL = 1000   # ms, each coordinator -> its own leaves

# Battery-ordered election timeouts.  Longer range than baseline Raft/ECHO
# because the adaptive ping interval can grow to ECHOD_PING_MAX_INTERVAL.
ECHOD_ELECTION_TIMEOUT_MIN = 300
ECHOD_ELECTION_TIMEOUT_MAX = 600
ELECTION_TIE_BREAK_MS = 30     # deterministic per-node spread (hash of node_id)

# Leader handoff: below this battery % the leader nominates a successor
# directly (TimeoutNow-style) instead of waiting for an election timeout.
T_HANDOFF = 20

# ---------------------------------------------------------------------------
# Energy model (normalised units, 0-1 battery scale)
# ---------------------------------------------------------------------------
ENERGY_TX_COST = 0.0001    # battery units per message sent
ENERGY_RX_COST = 0.00002   # battery units per message received

# Role-based idle drain multipliers (applied to --battery-drain rate)
DRAIN_MULT_LEADER = 3.0
DRAIN_MULT_CANDIDATE = 2.0
DRAIN_MULT_FOLLOWER = 1.0
DRAIN_MULT_OBSERVER = 0.2
DRAIN_MULT_LEAF = 0.5
