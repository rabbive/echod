# Contributing to echod

This is a research codebase — the bar is "correct, measured, and honest about tradeoffs,"
not "production-hardened." That said, a few conventions keep the Raft/ECHO/echoD
comparison meaningful and the codebase easy for the next contributor (human or AI) to work in.

## Before you start

- Read [`CLAUDE.md`](CLAUDE.md) for repo layout, commands, and code conventions.
- Read [`PRD.md`](PRD.md) for what's already done vs. still planned — check the milestone table before assuming a phase is or isn't implemented.
- If you're touching protocol behavior (`simulation/protocols/`), skim the relevant [ADR](docs/decisions/) first — several non-obvious design choices (tiered architecture, battery-ordered elections, coordinators-only traffic, the seeded workload harness) exist *because* a simpler alternative was tried and measured worse. Don't revert one of these without re-reading why it's there.

## Branching and commits

- Branch off `main`; use a descriptive branch name (e.g. `claude/<feature>-<id>` for AI-assisted branches, otherwise `<type>/<short-description>`).
- Prefer new commits over amending existing ones once pushed.
- Commit messages: short imperative summary line, then (if needed) a body explaining *why*, not *what* — the diff already shows what changed.
- Update [`CHANGELOG.md`](CHANGELOG.md) under `[Unreleased]` for any user-visible or protocol-behavior change.

## Config discipline

**Never hardcode timing, energy, or consensus constants.** Every such value lives in
`simulation/core/config.py` (Phase 1) or `rpi/config.py` (Phase 2) — add new constants
there, imported everywhere they're used. This applies equally to shared Raft/ECHO
constants and echoD-specific ones (`BATCH_WINDOW_MS`, `T_HANDOFF`, etc.).

## Protocol changes must stay comparable

If you change Raft, ECHO, or echoD in a way that affects message counts, latency, or
energy:

1. Re-run `python3 -m simulation.main --duration 30 --seed 42 --charts --output-dir results` and check whether the headline numbers in the README / `docs/ECHOD_VS_RAFT_ECHO.md` still hold.
2. If they no longer hold, update those docs — don't leave stale measured numbers next to changed code.
3. Route any new traffic through the existing seeded workload (`simulation/workload.py`) rather than ad hoc test-only traffic — see [ADR 0004](docs/decisions/0004-seeded-workload-harness.md) for why this matters.
4. If the change affects echoD specifically, confirm it doesn't violate the coordinators-only-consensus-traffic invariant ([ADR 0003](docs/decisions/0003-coordinators-only-consensus-traffic.md)) unless that's the explicit intent.

## Tests

- All new protocol behavior needs a test in `simulation/tests/` — see [`docs/TESTING.md`](docs/TESTING.md) for what each existing test file covers and where a new test belongs.
- Tests are `pytest-asyncio` — use `async def test_...` and the existing fixtures in `conftest.py`.
- Run the full suite before opening a PR:
  ```bash
  python3 -m pytest simulation/tests/ rpi/tests/ -v
  ```
- CI runs on Python 3.10 and 3.12 (`fail-fast: false`) — both must pass.

## Pull requests

- Check for a PR template before writing the description; none exists in this repo currently, so a clear summary + test plan is sufficient.
- Run the full test suite locally first — don't rely on CI to catch failures you could have caught yourself.
- If your change affects Phase 2 (`rpi/`) or Phase 3 (`esp32/`), say so explicitly in the PR description — those tiers are lower-coverage than Phase 1 and reviewers should know to look more carefully.

## Style

- Type hints everywhere (PEP 484), docstrings on all classes and public methods, async/await throughout.
- No formatter is configured (no black/flake8/mypy) — match the existing style by hand rather than introducing a new one unannounced.
- `python3`, never bare `python` — the environment doesn't alias it.
