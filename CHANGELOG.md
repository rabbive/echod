# Changelog

All notable changes to this project are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); dates are commit dates, not release dates
(this project doesn't currently cut versioned releases).

---

## [Unreleased]

### Added
- `PRD.md` — project goals, functional requirements per phase, success metrics, milestone tracking.
- `CHANGELOG.md`, `docs/decisions/` (ADRs), `CONTRIBUTING.md`, `docs/TESTING.md`, `STATUS.md` — additional AI-agent/contributor context docs.

### Changed
- `CLAUDE.md` rewritten to cover all three protocols (Raft, ECHO, echoD) instead of just Raft/ECHO, including echoD's config constants, updated CLI flags, and explicit notes that Phase 2/3 are still ECHO-only.

---

## 2026-07 — echoD hybrid protocol

### Added
- `simulation/protocols/echod.py` — `EchoDCoordinator` / `EchoDLeaf`, the Raft/ECHO hybrid, implementing six efficiency optimizations over ECHO:
  1. Edge-side delta filtering (leaf-side, not coordinator-side)
  2. Batched event-driven consensus (burst of *k* events → 1 round)
  3. Coordinators-only adaptive liveness pings (exponential backoff, leaves get slow separate keepalives)
  4. Battery-ordered election timeouts (highest battery always wins first ballot)
  5. Directed leader handoff below `T_HANDOFF` battery
  6. Batched reconciliation after partition heal
- `simulation/workload.py` — deterministic seeded workload generator, replayed identically across Raft/ECHO/echoD so comparisons are fair.
- `simulation/tests/test_echod.py` — dedicated echoD test coverage (edge filtering, batching, adaptive liveness, battery-ordered timeouts, handoff, batched reconciliation).
- `docs/ECHOD_VS_RAFT_ECHO.md` — full design comparison, measured results (5s and 30s runs), and prior-art / novelty analysis.
- `docs/diagrams/*` — architecture, message-flow, batching, election, handoff, and partition diagrams (Excalidraw sources + rendered SVGs), plus `scripts/generate_diagrams.py` to regenerate them.
- New CLI flags on `simulation.main`: `--seed`, `--burst-interval`, `--no-workload`.
- New config constants in `simulation/core/config.py`: `BATCH_WINDOW_MS`, `MAX_BATCH_SIZE`, `ECHOD_PING_MAX_INTERVAL`, `LIVENESS_BACKOFF_FACTOR`, `LEAF_KEEPALIVE_INTERVAL`, `ECHOD_ELECTION_TIMEOUT_MIN/MAX`, `ELECTION_TIE_BREAK_MS`, `T_HANDOFF`, plus the energy model constants (`ENERGY_TX_COST`, `ENERGY_RX_COST`, `DRAIN_MULT_*`).

### Fixed (supporting fixes that made the 3-way comparison meaningful)
- Raft previously ran with zero client traffic in comparisons — it was measured doing nothing while ECHO did real work. Fixed by routing all three protocols through the same seeded workload.
- Energy model previously drained every node identically regardless of role — fixed with role-weighted idle drain + per-message TX/RX costs.
- Followers silently dropped forwarded sensor reports instead of relaying to the leader.
- Wakeup-precision bug: nodes polled their inbox on a fixed ~50ms quantum, which collapsed echoD's deliberately-staggered (30ms apart) battery-ordered election timeouts into lockstep split votes. Fixing this cut ECHO's own election churn from 84 RequestVotes down to 14 in one measured run.

### Measured (see `docs/ECHOD_VS_RAFT_ECHO.md` for full numbers)
- echoD vs ECHO: ~4.4× fewer messages (5s run), ~5–7× fewer messages (30s run).
- echoD availability 99.32% vs Raft 98.98% / ECHO 97.95%.
- echoD balances leader energy drain across the cluster (rotates leadership before any node dies) vs Raft's fixed leader draining to near-death (0.905) over 30s.

---

## 2026-07 — Dashboard overhaul and doc expansion

- `feat: overhaul dashboard UI and expand test/doc coverage` — Flask + Socket.IO dashboard improvements, additional Phase 2 tests.
- `docs: expand README and note dashboard stack in AGENTS`
- `Add comprehensive CLAUDE.md for AI assistant guidance` — original CLAUDE.md (Raft + ECHO only, pre-echoD).

---

## 2026-07 — CI and project scaffolding

- `ci: use actions/checkout@v6 and setup-python@v6 (Node 24)`
- `Add README, MIT license, and GitHub Actions CI`

---

## Earlier — Phase 1/2 foundation

- `Implement demo controls for mock battery management and enhance dashboard features`
- `Refactor MQTT connection handling and improve peer list management`
- `Enhance ECHO Phase 1 simulation with additional features and optimizations`
- `Add AGENTS.md with Cursor Cloud development instructions`
- `Initialize ECHO Phase 1 simulation` — original Raft + ECHO asyncio simulation.
