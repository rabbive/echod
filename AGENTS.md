# AGENTS.md

## Cursor Cloud specific instructions

This is a pure-Python asyncio simulation comparing Raft, ECHO, and echoD (a Raft/ECHO hybrid, `simulation/protocols/echod.py`) consensus protocols. There are no external services, databases, or web servers.

### Quick reference


| Task               | Command                                                                |
| ------------------ | ---------------------------------------------------------------------- |
| Install deps       | `pip install -r requirements.txt`                                      |
| Run tests          | `pytest simulation/tests/ -v`                                          |
| Run simulation     | `python3 -m simulation.main`                                           |
| Run with charts    | `python3 -m simulation.main --charts --output-dir results`             |
| Run with partition | `python3 -m simulation.main --partition-at 2 --heal-at 4 --duration 5` |
| Regenerate README diagrams | `python3 scripts/generate_diagrams.py` (needs network for SVG rendering via kroki.io; `--no-svg` for offline) |


### Non-obvious notes

- Use `python3` not `python` — this environment does not alias `python` to `python3`.
- `pip install` uses `--user` by default (installs to `~/.local/`). Ensure `$HOME/.local/bin` is on `PATH` for `pytest` to be found.
- The `--charts` flag requires `matplotlib` (already in `requirements.txt`). A harmless font warning about `NotoColorEmoji.ttf` appears on first run — it does not affect output.
- Output goes to `results/` by default (CSVs + PNGs). This directory is gitignored.
- Python 3.10+ is required (uses `X | Y` union syntax and `match` statements).

### Phase 2 hardware-free demo (`rpi/`, `scripts/demo.sh`)

- Run `bash scripts/demo.sh`; the dashboard may bind to **5001+** on macOS if **5000** is taken (AirPlay). The script waits until HTTP responds before printing “Demo running”.
- Coordinators honor MQTT `**demo_control`** only for **mock** battery unless `**ECHO_DEMO=0`** is set in the environment (disables those handlers).
- Optional: `**ECHO_DEMO_LOW_BATTERY=1`** or `**DEMO_BATTERY_BASE=N**` when starting the script to tune initial mock battery (see comments in `scripts/demo.sh`).
- Tests: `pytest simulation/tests/ rpi/tests/ -v`.

## Learned User Preferences

- Always keep `CURSOR_CONTEXT.md` out of the git repo (ensure it is ignored).
- Phase 2 dashboard UI should follow shadcn-style conventions and use no gradients.

## Learned Workspace Facts

- On PEP 668–managed system Python (common on macOS), run the hardware-free demo with a project venv: `python3 -m venv .venv`, `.venv/bin/pip install -r rpi/requirements.txt`, then `PATH="$(pwd)/.venv/bin:$PATH" bash scripts/demo.sh` so `python3` and dependencies resolve.
- For roughly the first minute after `scripts/demo.sh` starts, the dashboard may update often while coordinators finish leader election and leaves register or re-register; this usually settles once the cluster reaches steady state.
- The Phase 2 Flask dashboard template (`rpi/dashboard/templates/index.html`) uses Tailwind CDN, Geist Sans and Geist Mono from Fontsource, Chart.js, and Socket.IO for live cluster state.
- Phase 2 stable-demo timing defaults live in `rpi/config.py`: `ELECTION_TIMEOUT_MIN=1.2s`, `ELECTION_TIMEOUT_MAX=2.4s`, `LIVENESS_PING_INTERVAL=0.25s`. Values much smaller than this cause election churn under MQTT + laptop scheduler jitter.
- Default `MOCK_BATTERY_DRAIN_RATE` in `rpi/config.py` is `0.02` %/s so the Phase 2 demo stays stable for presentations; the dashboard exposes an input to bump it for energy-stress demos.
- Phase 2 logs are at `/tmp/echo-coord-*.log`, `/tmp/echo-leaves.log`, and `/tmp/echo-dashboard.log`; check these first when diagnosing election churn or dashboard issues.
- Constant leader/coordinator flipping in the Phase 2 demo is almost always ghost processes from a previous run. `bash scripts/demo.sh stop` now `pkill -f`s `rpi.coordinator.echo_node`, `rpi.mock_leaf`, and `rpi.dashboard.app`; verify with `ps` if churn persists after a restart.