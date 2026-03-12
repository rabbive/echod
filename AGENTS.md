# AGENTS.md

## Cursor Cloud specific instructions

This is a pure-Python asyncio simulation comparing ECHO and Raft consensus protocols. There are no external services, databases, or web servers.

### Quick reference

| Task | Command |
|---|---|
| Install deps | `pip install -r requirements.txt` |
| Run tests | `pytest simulation/tests/ -v` |
| Run simulation | `python3 -m simulation.main` |
| Run with charts | `python3 -m simulation.main --charts --output-dir results` |
| Run with partition | `python3 -m simulation.main --partition-at 2 --heal-at 4 --duration 5` |

### Non-obvious notes

- Use `python3` not `python` — this environment does not alias `python` to `python3`.
- `pip install` uses `--user` by default (installs to `~/.local/`). Ensure `$HOME/.local/bin` is on `PATH` for `pytest` to be found.
- The `--charts` flag requires `matplotlib` (already in `requirements.txt`). A harmless font warning about `NotoColorEmoji.ttf` appears on first run — it does not affect output.
- Output goes to `results/` by default (CSVs + PNGs). This directory is gitignored.
- Python 3.10+ is required (uses `X | Y` union syntax and `match` statements).
