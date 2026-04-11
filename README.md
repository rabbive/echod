# Echod

**Energy-aware clustered hierarchical consensus** — a research codebase that implements **ECHO**, a consensus protocol that extends **Raft** with **battery-aware voting**, and compares both under identical conditions. It includes a pure-Python **asyncio simulation** (no external services) and an optional **Phase 2** stack: coordinators and mock sensors over **MQTT**, with a **live web dashboard**.

![Python](https://img.shields.io/badge/python-3.10+-3776AB?logo=python&logoColor=white)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![CI](https://github.com/rabbive/echod/actions/workflows/ci.yml/badge.svg)](https://github.com/rabbive/echod/actions/workflows/ci.yml)

---

## What this project is

**Echod** is a toolkit for studying consensus in **resource-constrained** or **energy-aware** distributed systems:

| Piece | Role |
|--------|------|
| **ECHO protocol** | Like Raft (leader election, log replication), but nodes with very low **mock battery** can become **observers** — they leave the voting quorum so depleted nodes do not disrupt elections. Recovery thresholds are configurable (`T_low`, `T_restore`). |
| **Raft baseline** | The same simulation harness runs classic Raft so you can compare message counts, latency, availability, and energy side-by-side. |
| **Simulation** | Runs entirely in-process with asyncio: configurable coordinators, optional leaf tier, optional network partition windows, CSV metrics, optional matplotlib charts. |
| **Phase 2 (MQTT demo)** | Coordinators publish status over MQTT; a Flask + Socket.IO dashboard shows live cluster state; mock “leaf” nodes mimic ESP32-style sensors. **No hardware required** for the demo script. |

**Further reading**

- Architecture and design notes: [docs/ECHO_ARCHITECTURE.plan.md](docs/ECHO_ARCHITECTURE.plan.md)
- Dashboard, demo controls, ECHO vs Raft: [docs/ECHO_DEMO_AND_RAFT.md](docs/ECHO_DEMO_AND_RAFT.md)

---

## Prerequisites

| Requirement | Notes |
|-------------|--------|
| **Python 3.10+** | Uses `match` and `X \| Y` typing. |
| **pip** | Use `python3 -m pip` if needed. On macOS with PEP 668, prefer a **project venv** (see below). |
| **MQTT broker (Phase 2 only)** | [Mosquitto](https://mosquitto.org/) recommended — e.g. `brew install mosquitto` on macOS. The demo script starts it when available. |

---

## Step-by-step: clone and install

1. **Clone the repository**

   ```bash
   git clone https://github.com/rabbive/echod.git
   cd echod
   ```

2. **Create a virtual environment (recommended, especially on macOS)**

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate          # Windows: .venv\Scripts\activate
   ```

3. **Install Python dependencies**

   ```bash
   python3 -m pip install -r requirements.txt
   ```

   For the **MQTT demo and RPi tests**, also install Phase 2 deps:

   ```bash
   python3 -m pip install -r rpi/requirements.txt
   ```

4. **Optional:** ensure `pytest` is on your `PATH` (or run `python3 -m pytest`).

---

## Step-by-step: run the simulation

The simulation compares Raft and ECHO, prints summaries to the terminal, and writes metrics under `results/` (gitignored).

1. From the repo root (with venv activated if you use one):

   ```bash
   python3 -m simulation.main
   ```

2. **Optional — charts** (requires matplotlib, already listed in `requirements.txt`):

   ```bash
   python3 -m simulation.main --charts --output-dir results
   ```

3. **Optional — partition scenario** (example: split at 2 s, heal at 4 s, total run 5 s):

   ```bash
   python3 -m simulation.main --partition-at 2 --heal-at 4 --duration 5
   ```

**Useful CLI flags**

| Flag | Default | Description |
|------|---------|-------------|
| `--coordinators` | 5 | Number of coordinator peers |
| `--leaves` | 10 | ECHO leaf nodes |
| `--duration` | 5 | Run length (seconds) |
| `--battery-drain` | 0.01 | Normalized drain per second |
| `--partition-at` / `--heal-at` | — | Optional partition window |
| `--charts` | off | Write comparison PNGs |
| `--output-dir` | `results` | CSV / chart output directory |

---

## Step-by-step: hardware-free MQTT demo (live dashboard)

This starts (when possible) a local Mosquitto broker, five **mock-battery** coordinators, mock leaf nodes, and a **Flask dashboard** with Socket.IO updates.

1. Install both requirement files if you have not already:

   ```bash
   python3 -m pip install -r requirements.txt
   python3 -m pip install -r rpi/requirements.txt
   ```

2. **Run the launcher** (uses `python3` from your current environment):

   ```bash
   bash scripts/demo.sh
   ```

3. Wait until the script prints **“Demo running!”** — it polls HTTP until the dashboard is up.

4. **Open the dashboard** in a browser using the URL printed (often **`http://localhost:5000`**; on macOS, **5001+** is common if port 5000 is used by AirPlay).

5. **What to expect:** For roughly the first minute, nodes may elect a leader and leaves may re-register; activity then usually stabilizes. The UI shows coordinators, battery bars, leader/term stats, traffic charts, and **demo controls** (battery, drain, pause leaves) when coordinators run with mock battery.

6. **Stop everything:**

   ```bash
   bash scripts/demo.sh stop
   ```

**Logs:** `/tmp/echo-coord-*.log`, `/tmp/echo-leaves.log`, `/tmp/echo-dashboard.log`

**Optional environment** (see comments in `scripts/demo.sh`): e.g. `ECHO_DEMO_LOW_BATTERY=1`, `DEMO_BATTERY_BASE=N`, or `ECHO_DEMO=0` on coordinators to disable demo MQTT handlers.

**Strict venv usage on macOS:** If system Python blocks global installs, activate `.venv` and run:

```bash
PATH="$(pwd)/.venv/bin:$PATH" bash scripts/demo.sh
```

---

## Step-by-step: run tests

```bash
python3 -m pip install -r requirements.txt
python3 -m pip install -r rpi/requirements.txt
python3 -m pytest simulation/tests/ rpi/tests/ -v
```

---

## Repository layout

```
echod/
├── simulation/          # ECHO vs Raft asyncio simulation, metrics, CLI
├── rpi/                 # MQTT transport, coordinator, dashboard, mock leaves
├── scripts/demo.sh      # Local broker + cluster + dashboard launcher
├── docs/                # Architecture and demo deep-dives
├── requirements.txt     # Simulation + base test deps
└── rpi/requirements.txt # Phase 2 (Flask, MQTT, …)
```

---

## Documentation for agents and contributors

Day-to-day commands and environment quirks: [AGENTS.md](AGENTS.md). **`CURSOR_CONTEXT.md` is intentionally not committed** (local only).

---

## Contributing

Issues and pull requests are welcome. Please run the full test suite above before submitting changes.

---

## License

This project is licensed under the [MIT License](LICENSE).
