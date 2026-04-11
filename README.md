# Echod

**Energy-aware clustered hierarchical consensus** — a pure-Python research codebase that compares **ECHO** (energy-aware) and **Raft** under identical conditions, plus an optional **Phase 2** MQTT stack for a hardware-free cluster demo.

![Python](https://img.shields.io/badge/python-3.10+-3776AB?logo=python&logoColor=white)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![CI](https://github.com/rabbive/echod/actions/workflows/ci.yml/badge.svg)](https://github.com/rabbive/echod/actions/workflows/ci.yml)

---

## Overview

| Component | What it does |
|-------------|----------------|
| **`simulation/`** | Asyncio simulation: Raft vs ECHO side-by-side, metrics to CSV, optional matplotlib charts. No network or external services. |
| **`rpi/`** | MQTT-backed coordinators, live Flask + Socket.IO dashboard, mock leaf nodes — runs locally with Mosquitto for demos. |
| **`scripts/demo.sh`** | One command to start broker, coordinators, leaves, and the dashboard. |

ECHO extends classic consensus with **energy-aware** behavior (e.g. observer transitions when mock battery drops). The simulator makes protocol and energy metrics easy to compare in a controlled way.

**Architecture (simulation + MQTT, flowcharts):** [docs/ECHO_ARCHITECTURE.plan.md](docs/ECHO_ARCHITECTURE.plan.md)

**Deep dive (dashboard UI, demo controls, ECHO vs Raft):** [docs/ECHO_DEMO_AND_RAFT.md](docs/ECHO_DEMO_AND_RAFT.md)

---

## Requirements

- **Python 3.10+** (uses `match` and `X | Y` union syntax)
- **Simulation only:** dependencies in [`requirements.txt`](requirements.txt)
- **Phase 2 demo:** also install [`rpi/requirements.txt`](rpi/requirements.txt) and a local **MQTT broker** ([Mosquitto](https://mosquitto.org/) — e.g. `brew install mosquitto` on macOS)

---

## Quick start — simulation

```bash
pip install -r requirements.txt

# Run both protocols (Raft then ECHO), print summaries, write CSVs under results/
python3 -m simulation.main

# With charts (requires matplotlib)
python3 -m simulation.main --charts --output-dir results

# Partition scenario (example: partition at 2s, heal at 4s, 5s total)
python3 -m simulation.main --partition-at 2 --heal-at 4 --duration 5
```

CLI highlights:

| Flag | Default | Description |
|------|---------|-------------|
| `--coordinators` | 5 | Number of Raft / ECHO coordinator peers |
| `--leaves` | 10 | ECHO leaf nodes |
| `--duration` | 5 | Simulation length (seconds) |
| `--battery-drain` | 0.01 | Drain rate per second (normalized) |
| `--partition-at` / `--heal-at` | — | Optional split-brain window |
| `--charts` | off | Emit comparison PNGs |
| `--output-dir` | `results` | CSV/chart output (gitignored) |

---

## Quick start — hardware-free MQTT demo (Phase 2)

```bash
pip install -r requirements.txt
pip install -r rpi/requirements.txt

bash scripts/demo.sh
```

Then open the dashboard URL printed in the terminal (often **`http://localhost:5000`**; on macOS **port 5001+** is common if AirPlay uses 5000). The script waits until the dashboard responds over HTTP.

- **Stop:** `bash scripts/demo.sh stop`
- **Logs:** `/tmp/echo-coord-*.log`, `/tmp/echo-leaves.log`, `/tmp/echo-dashboard.log`
- **Optional env:** see comments in [`scripts/demo.sh`](scripts/demo.sh) (e.g. `ECHO_DEMO_LOW_BATTERY`, `DEMO_BATTERY_BASE`, `ECHO_DEMO=0` for coordinators)

The web UI includes **demo controls** (mock battery, drain, leaf pause) when coordinators run with `--mock`.

---

## Tests

```bash
pip install -r requirements.txt
pip install -r rpi/requirements.txt   # needed for rpi/tests

python3 -m pytest simulation/tests/ rpi/tests/ -v
```

---

## Repository layout

```
echod/
├── simulation/          # ECHO vs Raft simulation core, metrics, CLI
├── rpi/                 # MQTT transport, coordinator, dashboard, mock leaves
├── scripts/demo.sh      # Local cluster + dashboard launcher
├── requirements.txt     # Simulation + test deps
└── rpi/requirements.txt # Phase 2 (Flask, MQTT, …)
```

---

## Documentation for agents / contributors

Operational notes for automation and humans live in [`AGENTS.md`](AGENTS.md) (commands, environment quirks, Phase 2 demo). **`CURSOR_CONTEXT.md` is intentionally not committed** (local only).

---

## Contributing

Issues and pull requests are welcome. Please run the full test suite above before submitting changes.

---

## License

This project is licensed under the [MIT License](LICENSE).
