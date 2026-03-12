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
