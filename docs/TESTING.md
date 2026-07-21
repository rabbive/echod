# Testing Guide

What each test file validates, and why — so a new test lands in the right place and
existing failures are easy to interpret.

```bash
# Everything
python3 -m pytest simulation/tests/ rpi/tests/ -v

# Simulation only / echoD only / RPi only
python3 -m pytest simulation/tests/ -v
python3 -m pytest simulation/tests/test_echod.py -v
python3 -m pytest rpi/tests/ -v
```

All simulation tests are `pytest-asyncio` (`async def test_...`); fixtures live in
`simulation/tests/conftest.py`.

---

## `simulation/tests/test_election.py`

Leader election correctness for **Raft and ECHO** (echoD's election behavior is
covered separately in `test_echod.py` since it uses a different, deterministic
timeout scheme — see [ADR 0002](decisions/0002-battery-ordered-elections.md)).

- `test_raft_elects_leader` / `test_raft_single_term_leader` — a Raft cluster converges on exactly one leader per term.
- `test_echo_elects_leader` — same, for ECHO's tiered setup.
- `test_echo_observer_cannot_win_election` — a node in observer mode (`NodeState.OBSERVER`, battery < `T_LOW`) never becomes leader, even if it would otherwise win.
- `test_echo_rejects_low_battery_candidate` — coordinators refuse to vote for a candidate below `T_LOW`, regardless of log state.
- `test_echo_energy_weighted_scoring` — ECHO's tie-break scoring (`election_score()`) picks the higher-battery candidate on a vote-count tie. (This path is largely superseded in echoD by deterministic battery-ordered timeouts — see ADR 0002 — but ECHO itself still needs it tested.)

Also exports `wait_for_leader`, a shared polling helper reused by `test_echod.py`.

## `simulation/tests/test_energy.py`

Battery/energy state transitions, independent of election outcome.

- `TestBatteryDrain` — idle drain reduces battery over time at the expected rate; role-weighted multipliers (`DRAIN_MULT_LEADER`, etc.) apply correctly.
- `TestObserverTransition` — a coordinator crossing below `T_LOW` steps down into observer mode; crossing back above `T_RESTORE` rejoins consensus. Covers both directions and that partial recovery (between `T_LOW` and `T_RESTORE`) does *not* rejoin early.
- `TestStepDown` — a leader/candidate observing a higher term steps down to follower, independent of battery (standard Raft safety, not energy-specific).

## `simulation/tests/test_partition.py`

Network partition and recovery, for protocols with partition-tolerant provisional
consensus (ECHO/echoD).

- `test_partition_drops_messages` — `MessageBus` actually enforces the partition topology (messages between split sub-clusters are dropped).
- `test_heal_restores_connectivity` — healing a partition restores message delivery between previously-split nodes.
- `test_provisional_mode_sets_epoch` — detecting a partition increments `partition_epoch` and (if applicable) demotes `LEADER` to `LOCAL_LEADER`.
- `test_exit_provisional_resets_epoch` — healing resets `partition_epoch` to 0 and returns a local leader to follower.
- `test_reconcile_replays_provisional_entries` — the losing side of a reconciliation truncates to the winning commit index and replays its provisional entries as new proposals (one-by-one in ECHO; as a single batch in echoD — see `test_echod.py`'s `TestBatchedReconciliation`).

## `simulation/tests/test_echod.py`

echoD-specific behavior — the six optimizations from
[`docs/ECHOD_VS_RAFT_ECHO.md`](ECHOD_VS_RAFT_ECHO.md), each with its own test class:

- `TestEdgeFiltering` (opt 1) — `EchoDLeaf` suppresses sub-threshold readings before transmission; only breaching readings are sent.
- `TestBatching` (opt 2) — multiple triggers arriving within `BATCH_WINDOW_MS` coalesce into a single log entry; a full batch (`MAX_BATCH_SIZE`) flushes immediately without waiting for the window to close.
- `TestAdaptiveLiveness` (opt 3) — the leader's ping interval backs off from `LIVENESS_PING_INTERVAL` toward `ECHOD_PING_MAX_INTERVAL` while idle and snaps back on consensus activity; pings go to coordinators only, never leaves.
- `TestBatteryOrderedTimeouts` (opt 4) — `_new_election_deadline()` produces shorter timeouts for higher battery, and the deterministic tie-break is stable per node (not randomized between runs).
- `TestHandoff` (opt 5) — a leader below `T_HANDOFF` sends a directed `LeadershipHandoff` to its highest-battery peer and steps down; the nominee starts an election on receipt.
- `TestBatchedReconciliation` (opt 6) — provisional entries replay as one batch log entry after partition heal, not one round per entry.
- `TestBuilder` — `build_echod_cluster` wires up the expected number of `EchoDCoordinator`/`EchoDLeaf` nodes.

If you add a seventh optimization or change one of the six, add or update the
matching test class here — don't bury echoD-specific coverage in `test_election.py`
or `test_partition.py`, since those are meant to stay protocol-agnostic across
Raft/ECHO.

## `simulation/tests/test_cli_runner.py`

The `simulation.main` CLI end-to-end: `test_partition_at_zero_injects_partition`
covers an edge case (partition scheduled at simulation start) rather than
re-testing all CLI flags — flag parsing itself is simple enough not to need
per-flag tests.

## `rpi/tests/test_battery_monitor.py`

Phase 2 mock/real battery monitor (`rpi/coordinator/battery.py`):
`test_set_mock_level_clamps`, `test_set_mock_drain_paused_freezes_level`,
`test_demo_control_set_battery_updates_level`, `test_demo_control_drain_actions` —
the dashboard's demo battery controls (`ECHO_DEMO_LOW_BATTERY`, pause/resume drain,
manual level set) behave correctly and stay within `[0, 100]`.

## `rpi/tests/test_mqtt_dispatch_queue.py`

`test_dispatch_enqueues_and_processes_in_event_loop` — MQTT callbacks (which fire on
paho-mqtt's own thread) correctly hand off to the asyncio event loop via a dispatch
queue, rather than touching asyncio state from the wrong thread.

---

## Coverage gaps (known, not yet addressed)

- No test currently exercises echoD or Raft over the **Phase 2 MQTT transport** — `rpi/tests/` only covers the battery monitor and the MQTT dispatch mechanism, not protocol behavior over real MQTT. This is expected: Phase 2's coordinator stack is ECHO-only right now (see `PRD.md` FR2.2/FR2.3).
- No automated tests exist for `esp32/main/` firmware (C, requires ESP-IDF + hardware or an emulator) — out of scope for `pytest`, tracked as a Phase 3 gap in `PRD.md`.
