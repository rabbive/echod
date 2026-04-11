# CLAUDE.md

AI assistant guide for the **echod** repository — ECHO consensus protocol research project.

---

## Project Overview

**echod** is a research project implementing and comparing two consensus protocols:

- **ECHO** — an energy-aware, tiered consensus protocol designed for IoT environments
- **Raft** — the standard Raft consensus algorithm (used as a performance baseline)

The project has three deployment tiers:

| Tier | Description | Location |
|------|-------------|----------|
| **Phase 1** | Pure-Python asyncio simulation (no hardware, no network) | `simulation/` |
| **Phase 2** | Hardware-free MQTT demo (Raspberry Pi-style, mock sensors) | `rpi/`, `scripts/demo.sh` |
| **Phase 3** | Real ESP32 leaf node firmware (optional, requires hardware) | `esp32/` |

---

## Repository Structure

```
echod/
├── simulation/           # Phase 1: pure asyncio simulation
│   ├── core/             # Shared primitives
│   │   ├── config.py     # ALL timing/energy/consensus constants live here
│   │   ├── node.py       # Base Node + Coordinator + Leaf classes
│   │   ├── cluster.py    # Cluster orchestrator + event loop management
│   │   ├── messages.py   # Frozen dataclass message types
│   │   └── log.py        # ReplicatedLog (1-based indexing, Raft convention)
│   ├── protocols/        # Protocol-specific implementations
│   │   ├── raft.py       # RaftNode: standard election + log replication
│   │   └── echo.py       # EchoCoordinator + EchoLeaf: energy-aware consensus
│   ├── metrics/          # Metrics collection and CSV/PNG reporting
│   ├── tests/            # Async unit + integration tests (pytest-asyncio)
│   └── main.py           # CLI entry point
│
├── rpi/                  # Phase 2: MQTT-based hardware demo
│   ├── coordinator/
│   │   ├── echo_node.py  # ECHO coordinator over MQTT transport
│   │   ├── transport.py  # paho-mqtt wrapper (handles v1.x/v2.x differences)
│   │   └── battery.py    # Real ADS1115 ADC or mock battery monitor
│   ├── dashboard/
│   │   └── app.py        # Flask + Socket.IO real-time monitoring UI
│   ├── mock_leaf.py      # Simulates ESP32 leaf (no hardware needed)
│   ├── config.py         # Phase 2 config (MQTT broker, energy, dashboard)
│   ├── tests/            # Battery monitor tests
│   └── requirements.txt  # Phase 2 extra deps (flask, paho-mqtt, cryptography)
│
├── esp32/                # Phase 3: ESP32 firmware (C + ESP-IDF, optional)
│   └── main/             # CMake project; DHT22 sensor + WiFi + MQTT
│
├── scripts/
│   ├── demo.sh           # One-command demo launcher (broker + nodes + dashboard)
│   ├── deploy_rpi.sh     # Raspberry Pi deployment helper
│   └── run_simulation.sh # Convenience wrapper for simulation
│
├── .github/workflows/
│   └── ci.yml            # GitHub Actions: pytest on Python 3.10 + 3.12
│
├── requirements.txt      # Phase 1 deps: pytest, pytest-asyncio, matplotlib
├── AGENTS.md             # Legacy Cursor Cloud notes (kept for reference)
├── README.md             # User-facing quick-start guide
└── LICENSE               # MIT
```

---

## Quick Commands

### Phase 1 — Simulation

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
python3 -m pytest simulation/tests/ rpi/tests/ -v

# Run basic simulation (5 coordinators, 10 leaves, 5 seconds)
python3 -m simulation.main

# Run with partition injection and chart output
python3 -m simulation.main --partition-at 2 --heal-at 4 --duration 5 --charts --output-dir results

# CLI flags
#   --coordinators N     Number of peer coordinators (default: 5)
#   --leaves N           Number of ECHO leaf nodes (default: 10)
#   --duration S         Simulation length in seconds (default: 5)
#   --battery-drain R    Drain rate per second 0–1 (default: 0.01)
#   --partition-at T     Inject network partition at T seconds
#   --heal-at T          Heal partition at T seconds
#   --charts             Generate PNG comparison charts
#   --output-dir PATH    Output dir for CSVs/charts (default: results/)
```

### Phase 2 — Hardware-Free MQTT Demo

```bash
# Install all deps
pip install -r requirements.txt -r rpi/requirements.txt

# Launch full demo (broker + 5 coordinators + 5 leaves + dashboard)
bash scripts/demo.sh

# Stop demo
bash scripts/demo.sh stop

# Dashboard: http://localhost:5000 (may bind to 5001+ on macOS if 5000 is taken)
```

---

## Environment Variables (Phase 2)

| Variable | Default | Description |
|----------|---------|-------------|
| `ECHO_BROKER_HOST` | `localhost` | MQTT broker hostname |
| `ECHO_BROKER_PORT` | `1883` | MQTT broker port |
| `ECHO_CLUSTER_ID` | `echo-default` | Cluster identifier prefix |
| `ECHO_DASHBOARD_HOST` | `0.0.0.0` | Dashboard bind address |
| `ECHO_DASHBOARD_PORT` | `5000` | Dashboard HTTP port |
| `ECHO_DEMO_LOW_BATTERY=1` | — | Start coordinators at ~12–28% mock battery |
| `DEMO_BATTERY_BASE=N` | — | Custom battery range `[N, N+19]` |
| `ECHO_DEMO=0` | — | Disable demo battery controls on coordinators |

---

## Architecture

### Phase 1 Protocol Design

**ECHO (tiered, energy-aware)**:
- **Coordinators**: Full consensus participants; energy-weighted voting
- **Leaves**: Lightweight sensor reporters; register with a coordinator
- **Observer mode**: When battery < `T_LOW` (15%), node steps down and only observes
- **Restore threshold**: Node rejoins consensus when battery recovers to `T_RESTORE` (25%)
- **Liveness pings**: ~32-byte lightweight keep-alive (replaces full heartbeats in idle)
- **Partition epochs**: Provisional consensus during network splits; reconciliation on heal
- **Event triggers**: `Delta`, `Join`, `Partition`, `Liveness`, `Reconcile`

**Raft (flat, standard)**:
- All nodes are peers; no tiering
- Standard election timeout + `RequestVote` / `AppendEntries` RPCs
- Used as performance and correctness baseline

**Message types** (frozen dataclasses in `simulation/core/messages.py`):
- `RequestVoteRPC` / `RequestVoteResponse`
- `AppendEntriesRPC` / `AppendEntriesResponse`
- `LivenessPing`
- `LeafRegisterRequest` / `LeafRegisterResponse`
- `SensorDataReport`

**Cluster orchestration** (`simulation/core/cluster.py`):
- `MessageBus`: In-memory router; enforces partition topology; hooks for metrics
- `Cluster`: Spins up all nodes, runs the asyncio event loop, collects results

### Key Configuration (`simulation/core/config.py`)

**All** timing, energy, and consensus constants live in this one file. Never hardcode these values elsewhere.

```python
ELECTION_TIMEOUT_MIN = 150   # ms
ELECTION_TIMEOUT_MAX = 300   # ms
LIVENESS_PING_INTERVAL = 50  # ms
PARTITION_TIMEOUT = 2000     # ms
T_LOW = 15                   # % battery — enter observer mode
T_RESTORE = 25               # % battery — rejoin consensus
DELTA_THRESHOLD = 0.05       # sensor delta triggering consensus round
MAX_COORDINATORS = 7
SAMPLE_INTERVAL = 100        # ms, sensor polling rate
MAX_RETRIES = 3
RETRY_BACKOFF_MS = 50
MESSAGE_SIGN = True
```

---

## Code Conventions

### Naming

- Node IDs follow `{protocol}-{index}` pattern: `raft-0`, `coord-1`, `leaf-3`
- Message types are **frozen dataclasses** (immutable during routing)
- Enums for state: `NodeState`, `LeafState`, `TriggerType`
- Log indexing is **1-based** (Raft convention)

### Style

- **Type hints everywhere** (PEP 484)
- **Docstrings** on all classes and public methods
- **Async/await** throughout; no callbacks except the message bus hooks
- `python3` — do not use `python`; the environment does not alias it
- No external formatter configured (no black/flake8/mypy) — follow existing style manually

### File Organization

- `core/` — shared abstractions, not protocol-specific
- `protocols/` — protocol-specific subclasses only
- `metrics/` — metric collection + reporting, kept separate from core
- `tests/` — colocated with the component they test (`simulation/tests/`, `rpi/tests/`)

### Key Rule: Config Centralization

> Never hardcode timing or energy values. Always import from `simulation/core/config.py` (Phase 1) or `rpi/config.py` (Phase 2).

---

## Testing

```bash
# All tests (simulation + rpi)
python3 -m pytest simulation/tests/ rpi/tests/ -v

# Simulation only
python3 -m pytest simulation/tests/ -v

# RPi/battery monitor only
python3 -m pytest rpi/tests/ -v
```

Tests are async (`pytest-asyncio`) and cover:
- Leader election under normal conditions
- Energy transitions (observer mode, restore)
- Network partition + reconciliation scenarios
- Battery monitor (mock mode)

CI runs the full suite on **Python 3.10** and **3.12** via GitHub Actions (`.github/workflows/ci.yml`). Tests must pass on both versions before merging.

---

## CI/CD

- **Trigger**: push or PR to `main`/`master`
- **Matrix**: Python 3.10, 3.12 (`fail-fast: false`)
- **Steps**: checkout → setup-python → `pip install` both `requirements.txt` files → `pytest`
- Uses `actions/checkout@v6` and `actions/setup-python@v6` (Node 24 runner, avoids Node 20 deprecation)

---

## Development Branch

Active development happens on branch `claude/add-claude-documentation-Dvls5`. The stable branch is `main`.

---

## Known Quirks and Gotchas

- **`python` vs `python3`**: Always use `python3`; `python` is not aliased in this environment.
- **`pip install --user`**: Installs to `~/.local/`; ensure `$HOME/.local/bin` is on `PATH` so `pytest` is found.
- **Font warning**: `NotoColorEmoji.ttf` warning appears on first `--charts` run — harmless, ignore it.
- **Dashboard port on macOS**: AirPlay may occupy port 5000; the demo script will bind to 5001+ automatically.
- **`results/` directory**: gitignored; CSVs and charts go here by default.
- **`CURSOR_CONTEXT.md`**: gitignored intentionally — local-only reference file, do not commit.
- **paho-mqtt compatibility**: `rpi/coordinator/transport.py` handles both v1.x and v2.x callback API differences.
- **Phase 3 (ESP32)**: Requires ESP-IDF toolchain and real hardware; not part of normal dev/test workflow.
