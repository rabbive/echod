# CLAUDE.md

AI assistant guide for the **echod** repository — a research project comparing three consensus protocols.

---

## Project Overview

**echod** implements and compares **three** consensus protocols under one identical simulation harness:

- **Raft** — the standard consensus algorithm, used as the performance/correctness baseline
- **ECHO** — an energy-aware, tiered consensus protocol for IoT environments (extends Raft with battery-gated voting)
- **echoD** — a Raft/ECHO **hybrid**, built after measuring ECHO's own inefficiencies, that beats *both* parents on messages, latency, availability, and leader energy balance

The project has three deployment tiers:

| Tier | Description | Location | Status |
|------|-------------|----------|--------|
| **Phase 1** | Pure-Python asyncio simulation, all 3 protocols, seeded workload harness | `simulation/` | ✅ Complete |
| **Phase 2** | Hardware-free MQTT demo (Raspberry Pi-style, mock sensors) | `rpi/`, `scripts/demo.sh` | ⚠️ ECHO only — no echoD/Raft port yet |
| **Phase 3** | Real ESP32 leaf node firmware | `esp32/` | ⏳ Planned — leaf-only firmware exists, no coordinator firmware, no protocol comparison on real hardware |

**Important:** Phase 2's coordinator/dashboard stack (`rpi/coordinator/echo_node.py`) and Phase 3's firmware (`esp32/main/`) currently implement **ECHO only**. Porting echoD (and running a real Raft baseline) to either tier is still outstanding work — do not assume they exist without checking.

---

## Repository Structure

```
echod/
├── simulation/            # Phase 1: pure asyncio simulation, all 3 protocols
│   ├── core/               # Shared primitives
│   │   ├── config.py       # ALL timing/energy/consensus constants (incl. echoD-specific)
│   │   ├── node.py         # Base Node class
│   │   ├── coordinator.py  # CoordinatorNode: election + log replication machinery
│   │   ├── leaf.py         # LeafNode: sensor reporting, registration
│   │   ├── cluster.py      # Cluster orchestrator + event loop management
│   │   ├── messages.py     # Frozen dataclass message types
│   │   └── log.py          # ReplicatedLog (1-based indexing, Raft convention)
│   ├── protocols/          # Protocol-specific implementations
│   │   ├── raft.py         # RaftNode: standard election + log replication
│   │   ├── echo.py         # EchoCoordinator + EchoLeaf: energy-aware consensus
│   │   └── echod.py        # EchoDCoordinator + EchoDLeaf: the hybrid (6 optimizations)
│   ├── workload.py         # Deterministic seeded event generator — identical
│   │                       # workload replayed across Raft/ECHO/echoD for fair comparison
│   ├── metrics/            # Metrics collection and CSV/PNG reporting
│   │   ├── collector.py    # Tracks latency, message counts (by type), energy, availability
│   │   └── reporter.py     # CSV export + matplotlib comparison charts
│   ├── tests/              # Async unit + integration tests (pytest-asyncio)
│   │   └── test_echod.py   # echoD-specific tests (batching, handoff, adaptive liveness, etc.)
│   └── main.py             # CLI entry point — runs Raft, ECHO, and echoD back-to-back
│
├── rpi/                    # Phase 2: MQTT-based hardware demo (ECHO only, currently)
│   ├── coordinator/
│   │   ├── echo_node.py    # ECHO coordinator over MQTT transport
│   │   ├── transport.py    # paho-mqtt wrapper (handles v1.x/v2.x differences)
│   │   └── battery.py      # Real ADS1115 ADC or mock battery monitor
│   ├── dashboard/
│   │   └── app.py          # Flask + Socket.IO real-time monitoring UI
│   ├── mock_leaf.py        # Simulates ESP32 leaf (no hardware needed)
│   ├── config.py           # Phase 2 config (MQTT broker, energy, dashboard)
│   ├── tests/              # Battery monitor + MQTT dispatch tests
│   └── requirements.txt    # Phase 2 extra deps (flask, paho-mqtt, cryptography)
│
├── esp32/                  # Phase 3: ESP32 firmware (C + ESP-IDF, leaf-only, ECHO only)
│   └── main/               # CMake project; DHT22 sensor + WiFi + MQTT
│       ├── echo_leaf.c/h   # ECHO leaf behavior (registration, delta reporting)
│       ├── sensors.c/h     # DHT22 sensor reads
│       └── transport.c/h   # WiFi + MQTT transport
│
├── docs/
│   ├── ECHOD_VS_RAFT_ECHO.md   # Design comparison, measured results, prior-art analysis
│   ├── ECHO_ARCHITECTURE.plan.md
│   ├── ECHO_DEMO_AND_RAFT.md
│   └── diagrams/               # Excalidraw sources + rendered SVGs used in README
│
├── scripts/
│   ├── demo.sh                 # One-command demo launcher (broker + nodes + dashboard)
│   ├── deploy_rpi.sh           # Raspberry Pi deployment helper
│   ├── run_simulation.sh       # Convenience wrapper for simulation
│   └── generate_diagrams.py    # Regenerates docs/diagrams/*.excalidraw + .svg
│
├── .github/workflows/
│   └── ci.yml              # GitHub Actions: pytest on Python 3.10 + 3.12
│
├── requirements.txt        # Phase 1 deps: pytest, pytest-asyncio, matplotlib
├── PRD.md                  # Product requirements — goals, scope, milestones, success criteria
├── AGENTS.md               # Legacy Cursor Cloud notes (kept for reference)
├── README.md               # User-facing quick-start + protocol comparison writeup
└── LICENSE                 # MIT
```

---

## Quick Commands

### Phase 1 — Simulation (Raft vs ECHO vs echoD)

```bash
# Install dependencies
pip install -r requirements.txt

# Run tests
python3 -m pytest simulation/tests/ rpi/tests/ -v

# Run all three protocols back-to-back (5 coordinators, 10 leaves, 5 seconds, seeded workload)
python3 -m simulation.main

# With charts and a longer run (the "paper story" numbers are at 30s)
python3 -m simulation.main --duration 30 --seed 42 --charts --output-dir results

# Partition injection
python3 -m simulation.main --partition-at 2 --heal-at 4 --duration 5

# CLI flags
#   --coordinators N     Number of coordinator / Raft peer nodes (default: 5)
#   --leaves N           Number of ECHO/echoD leaf nodes (default: 10)
#   --duration S         Simulation length in seconds (default: 5)
#   --battery-drain R    Base idle drain rate per second, role-weighted (default: 0.01)
#   --partition-at T     Inject network partition at T seconds
#   --heal-at T          Heal partition at T seconds
#   --seed N             Workload schedule + election timer seed (default: 42)
#   --burst-interval S   Seconds between workload bursts (default: 1.0)
#   --no-workload        Idle-cluster comparison (disable injected sensor events)
#   --charts             Generate PNG comparison charts
#   --output-dir PATH    Output dir for CSVs/charts (default: results/)
```

Every run replays the **same seeded workload** (`simulation/workload.py`) through all three protocols, so any difference in messages/latency/energy comes from the protocol, not the traffic — this is the harness that makes the Raft vs ECHO vs echoD numbers in `docs/ECHOD_VS_RAFT_ECHO.md` and the README comparable.

### Phase 2 — Hardware-Free MQTT Demo (ECHO only)

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

### Raft (flat, standard baseline)

- All nodes are equal peers; no tiering
- Standard election timeout + `RequestVote` / `AppendEntries` RPCs
- Leader sends continuous full-`AppendEntries` heartbeats to every peer — this is the main source of its message overhead

### ECHO (tiered, energy-aware — echoD's starting point)

- **Coordinators**: full consensus participants; energy-gated voting (battery < `T_LOW` → reject vote)
- **Leaves**: lightweight sensor reporters; register with a coordinator
- **Observer mode**: battery < `T_LOW` (15%) → node steps down, observes only
- **Restore threshold**: rejoins consensus at `T_RESTORE` (25%)
- **Liveness pings**: lightweight keep-alive, but **broadcast to every node including leaves** — measured at 75–80% of ECHO's total traffic, the main thing echoD fixes
- **Partition epochs**: provisional consensus during network splits; one-round-per-entry reconciliation on heal
- **Event triggers**: `Delta`, `Join`, `Partition`, `Liveness`, `Reconcile`

### echoD (the hybrid — `simulation/protocols/echod.py`)

Keeps ECHO's tiered architecture, energy gating, and provisional-consensus partition tolerance, and adds **six** targeted optimizations:

1. **Edge-side delta filtering** — the delta-threshold check moves from the coordinator to the leaf (`EchoDLeaf._send_reading`); sub-threshold readings are never transmitted (zero radio cost), instead of being filtered *after* the leaf already paid the transmission cost.
2. **Batched event-driven consensus** — the leader coalesces every trigger arriving within `BATCH_WINDOW_MS` into a single log entry (`_flush_batch`); a burst of *k* events costs one consensus round, not *k*. Full batches (`MAX_BATCH_SIZE`) flush immediately, so there's no added latency under load.
3. **Coordinators-only adaptive liveness** — the leader pings coordinators only (never leaves), backing off exponentially from `LIVENESS_PING_INTERVAL` up to `ECHOD_PING_MAX_INTERVAL` while idle, snapping back to the fast interval the moment consensus activity resumes. Each coordinator separately keepalives its *own* leaves at the slower `LEAF_KEEPALIVE_INTERVAL` — this also fixes an ECHO bug where leaves registered to a non-leader coordinator would flap between ACTIVE/SEARCHING.
4. **Battery-ordered election timeouts** — `timeout = ECHOD_ELECTION_TIMEOUT_MIN + (1 − battery) × spread + crc32(node_id) % ELECTION_TIE_BREAK_MS`. The highest-battery coordinator always times out first and wins on the first ballot — no split votes, no wasted RequestVote rounds, energy-optimal leader for free.
5. **Directed leader handoff** — below `T_HANDOFF` (20%) battery, the leader nominates its highest-battery peer directly via a `LeadershipHandoff` message instead of waiting for a randomized election — one message, no election-timeout availability gap.
6. **Batched reconciliation** — provisional entries (tagged with `partition_epoch`) replay as **one** batch entry after a partition heals, not one round per entry.

**Design rule:** in echoD, consensus traffic (RequestVote/AppendEntries/pings) never touches leaf nodes at all — it is coordinator-to-coordinator only.

Full writeup, measured numbers, and prior-art analysis: [`docs/ECHOD_VS_RAFT_ECHO.md`](docs/ECHOD_VS_RAFT_ECHO.md).

**Message types** (frozen dataclasses in `simulation/core/messages.py`):
- `RequestVoteRPC` / `RequestVoteResponse`
- `AppendEntriesRPC` / `AppendEntriesResponse`
- `LivenessPing`
- `LeadershipHandoff` (echoD only)
- `LeafRegisterRequest` / `LeafRegisterResponse`
- `SensorDataReport`

**Cluster orchestration** (`simulation/core/cluster.py`):
- `MessageBus`: In-memory router; enforces partition topology; hooks for metrics
- `Cluster`: Spins up all nodes, runs the asyncio event loop, collects results

### Key Configuration (`simulation/core/config.py`)

**All** timing, energy, and consensus constants live in this one file. Never hardcode these values elsewhere.

```python
# Shared / Raft & ECHO
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

# echoD-specific
BATCH_WINDOW_MS = 50               # opt 2: batching window
MAX_BATCH_SIZE = 10                 # opt 2: flush immediately at this size
ECHOD_PING_MAX_INTERVAL = 250       # opt 3: adaptive ping backoff cap (ms)
LIVENESS_BACKOFF_FACTOR = 2.0       # opt 3
LEAF_KEEPALIVE_INTERVAL = 1000      # opt 3: coordinator -> own leaves (ms)
ECHOD_ELECTION_TIMEOUT_MIN = 300    # opt 4 (longer than ECHO's — see tradeoff below)
ECHOD_ELECTION_TIMEOUT_MAX = 600    # opt 4
ELECTION_TIE_BREAK_MS = 30          # opt 4: deterministic per-node spread
T_HANDOFF = 20                      # opt 5: battery % that triggers handoff

# Energy model (normalised 0-1 battery scale)
ENERGY_TX_COST = 0.0001
ENERGY_RX_COST = 0.00002
DRAIN_MULT_LEADER = 3.0
DRAIN_MULT_CANDIDATE = 2.0
DRAIN_MULT_FOLLOWER = 1.0
DRAIN_MULT_OBSERVER = 0.2
DRAIN_MULT_LEAF = 0.5
```

**Known tradeoff:** echoD's election timeouts (300–600ms) are wider than Raft/ECHO's (150–300ms), required so the adaptive ping backoff doesn't trigger spurious elections. Worst-case failure detection is ~150ms slower in exchange for ~5× less idle traffic. State this tradeoff explicitly in any writeup — don't just report the wins.

---

## Code Conventions

### Naming

- Node IDs follow `{protocol}-{index}` pattern: `raft-0`, `coord-1`, `leaf-3` (ECHO and echoD both use `coord-N`/`leaf-N`; disambiguate by which builder created the cluster)
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
- `protocols/` — protocol-specific subclasses only (`echod.py` subclasses `EchoCoordinator`/`LeafNode` from `echo.py`/`core/leaf.py` — don't duplicate ECHO logic there, override just the six optimizations)
- `metrics/` — metric collection + reporting, kept separate from core
- `tests/` — colocated with the component they test (`simulation/tests/`, `rpi/tests/`)

### Key Rule: Config Centralization

> Never hardcode timing or energy values. Always import from `simulation/core/config.py` (Phase 1) or `rpi/config.py` (Phase 2). This applies to echoD's new constants (`BATCH_WINDOW_MS`, `T_HANDOFF`, etc.) exactly the same as the original ECHO ones.

---

## Testing

```bash
# All tests (simulation + rpi)
python3 -m pytest simulation/tests/ rpi/tests/ -v

# Simulation only
python3 -m pytest simulation/tests/ -v

# echoD-specific tests only
python3 -m pytest simulation/tests/test_echod.py -v

# RPi/battery monitor only
python3 -m pytest rpi/tests/ -v
```

Tests are async (`pytest-asyncio`) and cover:
- Leader election under normal conditions (all 3 protocols)
- Energy transitions (observer mode, restore)
- Network partition + reconciliation scenarios
- echoD-specific: batching, adaptive liveness backoff, battery-ordered elections, leader handoff, batched reconciliation
- Battery monitor (mock mode)

CI runs the full suite on **Python 3.10** and **3.12** via GitHub Actions (`.github/workflows/ci.yml`). Tests must pass on both versions before merging.

---

## CI/CD

- **Trigger**: push or PR to `main`/`master`
- **Matrix**: Python 3.10, 3.12 (`fail-fast: false`)
- **Steps**: checkout → setup-python → `pip install` both `requirements.txt` files → `pytest`
- Uses `actions/checkout@v6` and `actions/setup-python@v6` (Node 24 runner, avoids Node 20 deprecation)

---

## Known Quirks and Gotchas

- **`python` vs `python3`**: Always use `python3`; `python` is not aliased in this environment.
- **`pip install --user`**: Installs to `~/.local/`; ensure `$HOME/.local/bin` is on `PATH` so `pytest` is found.
- **Font warning**: `NotoColorEmoji.ttf` warning appears on first `--charts` run — harmless, ignore it.
- **Dashboard port on macOS**: AirPlay may occupy port 5000; the demo script will bind to 5001+ automatically.
- **`results/` directory**: gitignored; CSVs and charts go here by default.
- **`CURSOR_CONTEXT.md`**: gitignored intentionally — local-only reference file, do not commit.
- **paho-mqtt compatibility**: `rpi/coordinator/transport.py` handles both v1.x and v2.x callback API differences.
- **Wakeup precision matters**: nodes must wake exactly at their computed deadline, not on a fixed polling quantum — a coarse quantum collapses echoD's deliberately-staggered (30ms apart) battery-ordered timeouts back into lockstep split votes. This was a real bug that inflated ECHO's own election churn from 14 to 84 RequestVotes before it was fixed; don't reintroduce fixed-interval polling in `Cluster`/`Node` idle loops.
- **Fair comparison requires the seeded workload**: always drive protocol comparisons through `simulation/workload.py`'s `generate_workload`/`deliver_event`, not ad-hoc traffic — otherwise Raft/ECHO/echoD aren't being compared under identical conditions (this was also a real bug: Raft used to run with zero client traffic).
- **Phase 2/3 are ECHO-only right now**: don't assume `rpi/` or `esp32/` have echoD or Raft implementations just because Phase 1 does — check before referencing them.

---

## Development Branch

Active development happens on feature branches off `main` (e.g. `claude/hybrid-raft-echo-protocol-23aija` for the echoD work). The stable branch is `main`.
