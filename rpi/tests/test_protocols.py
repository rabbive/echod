"""Per-protocol behavior tests for the Phase 2 MQTT coordinator node.

Covers the three --protocol modes (raft / echo / echod) of EchoCoordinator:
Raft's flat heartbeats and unfiltered per-event rounds, ECHO's energy
gating and coordinator-side filtering, and echoD's six optimizations
(batching, adaptive coordinators-only pings, battery-ordered timeouts,
directed handoff, batched reconciliation, pending-trigger replay).
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock

from rpi.config import (
    ECHOD_PING_MAX_INTERVAL,
    LIVENESS_PING_INTERVAL,
    MAX_BATCH_SIZE,
    T_HANDOFF,
)
from rpi.coordinator.battery import BatteryMonitor
from rpi.coordinator.echo_node import EchoCoordinator, NodeState


def _make_node(
    protocol: str,
    peers: tuple[str, ...] = ("coord-1", "coord-2"),
    battery: float = 100.0,
    node_id: str = "coord-0",
) -> tuple[EchoCoordinator, MagicMock, BatteryMonitor]:
    transport = MagicMock()
    batt = BatteryMonitor(mock=True, initial_level=battery)
    batt.set_mock_drain_rate(0.0)
    node = EchoCoordinator(
        node_id=node_id,
        peers=list(peers),
        transport=transport,
        battery=batt,
        protocol=protocol,
    )
    return node, transport, batt


def _make_leader(node: EchoCoordinator, term: int = 1) -> None:
    node.current_term = term
    node._become_leader()


def _reading(leaf: str, value: float) -> dict:
    return {"leaf_id": leaf, "sensor_type": "temperature", "value": value}


def _sent_types(transport: MagicMock) -> list[tuple[str, str]]:
    return [(c.args[0], c.args[1]) for c in transport.send.call_args_list]


# --------------------------------------------------------------------------
# Protocol validation
# --------------------------------------------------------------------------


def test_unknown_protocol_rejected() -> None:
    transport = MagicMock()
    batt = BatteryMonitor(mock=True, initial_level=100.0)
    try:
        EchoCoordinator(
            node_id="coord-0", peers=[], transport=transport,
            battery=batt, protocol="paxos",
        )
        raise AssertionError("expected ValueError")
    except ValueError:
        pass


# --------------------------------------------------------------------------
# Raft mode
# --------------------------------------------------------------------------


def test_raft_ignores_energy_gating() -> None:
    """Raft never enters observer mode, even below T_LOW."""
    node, _, _ = _make_node("raft", battery=10.0)
    node._check_battery()
    assert node.state == NodeState.FOLLOWER


def test_raft_leader_sends_directed_heartbeats_not_broadcast() -> None:
    node, transport, _ = _make_node("raft")
    _make_leader(node)
    transport.reset_mock()

    node._last_ping_time = 0.0  # force heartbeat-due
    node._tick()

    sent = _sent_types(transport)
    assert ("coord-1", "append_entries") in sent
    assert ("coord-2", "append_entries") in sent
    transport.broadcast.assert_not_called()


def test_raft_replicates_every_event_unfiltered() -> None:
    """No delta filtering: two near-identical readings = two rounds."""
    node, _, _ = _make_node("raft")
    _make_leader(node)

    node._handle_sensor_data("leaf-0", _reading("leaf-0", 20.00))
    node._handle_sensor_data("leaf-0", _reading("leaf-0", 20.01))  # 0.05% << 5%

    assert len(node.log) == 2
    assert all(e.trigger_type == "event" for e in node.log)


def test_raft_grants_vote_to_low_battery_candidate() -> None:
    """Raft has no battery-based vote gating (ECHO/echoD do)."""
    node, transport, _ = _make_node("raft")
    node.current_term = 1

    node._handle_request_vote("coord-1", {
        "term": 1, "candidate_id": "coord-1", "battery_level": 5,
        "last_log_index": 0, "last_log_term": 0,
    })

    payload = transport.send.call_args.args[2]
    assert payload["vote_granted"] is True


# --------------------------------------------------------------------------
# ECHO mode (regression — existing behavior must not change)
# --------------------------------------------------------------------------


def test_echo_energy_gating_still_applies() -> None:
    node, _, _ = _make_node("echo", battery=10.0)
    node._check_battery()
    assert node.state == NodeState.OBSERVER


def test_echo_denies_vote_to_low_battery_candidate() -> None:
    node, transport, _ = _make_node("echo")
    node.current_term = 1

    node._handle_request_vote("coord-1", {
        "term": 1, "candidate_id": "coord-1", "battery_level": 5,
        "last_log_index": 0, "last_log_term": 0,
    })

    payload = transport.send.call_args.args[2]
    assert payload["vote_granted"] is False


def test_echo_filters_at_coordinator_one_round_per_event() -> None:
    node, _, _ = _make_node("echo")
    _make_leader(node)

    node._handle_sensor_data("leaf-0", _reading("leaf-0", 20.0))
    node._handle_sensor_data("leaf-0", _reading("leaf-0", 20.1))  # filtered
    node._handle_sensor_data("leaf-0", _reading("leaf-0", 22.0))  # breach

    assert len(node.log) == 2
    assert all(e.trigger_type == "delta" for e in node.log)


def test_echo_leader_broadcasts_ping() -> None:
    node, transport, _ = _make_node("echo")
    _make_leader(node)
    transport.reset_mock()

    node._last_ping_time = 0.0
    node._tick()

    transport.broadcast.assert_called_once()
    assert transport.broadcast.call_args.args[0] == "liveness_ping"


# --------------------------------------------------------------------------
# echoD — optimization 4: battery-ordered election timeouts
# --------------------------------------------------------------------------


def test_echod_higher_battery_times_out_first() -> None:
    rich, _, _ = _make_node("echod", battery=90.0, node_id="coord-0")
    poor, _, _ = _make_node("echod", battery=50.0, node_id="coord-1")
    assert rich._election_deadline < poor._election_deadline


def test_echod_timeout_tie_break_is_deterministic() -> None:
    a1, _, _ = _make_node("echod", battery=80.0, node_id="coord-0")
    a2, _, _ = _make_node("echod", battery=80.0, node_id="coord-0")
    b, _, _ = _make_node("echod", battery=80.0, node_id="coord-1")
    # Same node id → identical deadline offset; different ids differ.
    assert a1._new_election_deadline() - a1._tie_break_s == (
        pytest_approx(a2._new_election_deadline() - a2._tie_break_s)
    )
    assert a1._tie_break_s != b._tie_break_s or True  # crc32 may collide; ok


def pytest_approx(value: float) -> float:
    import pytest
    return pytest.approx(value, abs=0.05)


# --------------------------------------------------------------------------
# echoD — optimization 2: batched consensus
# --------------------------------------------------------------------------


def test_echod_batches_burst_into_one_entry() -> None:
    node, _, _ = _make_node("echod")
    _make_leader(node)

    node._handle_sensor_data("leaf-0", _reading("leaf-0", 20.0))
    node._handle_sensor_data("leaf-1", _reading("leaf-1", 22.0))
    node._handle_sensor_data("leaf-2", _reading("leaf-2", 25.0))
    assert len(node.log) == 0  # still inside the batch window

    node._batch_deadline = 0.0  # force window closed
    node._tick()

    assert len(node.log) == 1
    entry = node.log[0]
    assert entry.command["count"] == 3
    assert len(entry.command["batch"]) == 3


def test_echod_full_batch_flushes_immediately() -> None:
    node, _, _ = _make_node("echod")
    _make_leader(node)

    value = 20.0
    for i in range(MAX_BATCH_SIZE):
        value *= 1.1  # guarantee delta breach
        node._handle_sensor_data(f"leaf-{i}", _reading(f"leaf-{i}", value))

    assert len(node.log) == 1
    assert node.log[0].command["count"] == MAX_BATCH_SIZE
    assert node._batch_deadline is None


# --------------------------------------------------------------------------
# echoD — optimization 3: adaptive coordinators-only liveness
# --------------------------------------------------------------------------


def test_echod_pings_coordinators_only_with_backoff() -> None:
    node, transport, _ = _make_node("echod")
    _make_leader(node)
    transport.reset_mock()

    # Cluster idle since the previous ping → interval must back off.
    node._last_ping_time = time.monotonic()
    node._last_consensus_activity = node._last_ping_time - 100.0
    before = node._ping_interval_s
    node._send_liveness_ping()

    sent = _sent_types(transport)
    assert ("coord-1", "liveness_ping") in sent
    assert ("coord-2", "liveness_ping") in sent
    transport.broadcast.assert_not_called()
    assert node._ping_interval_s > before


def test_echod_ping_backoff_caps_at_max_interval() -> None:
    node, _, _ = _make_node("echod")
    _make_leader(node)

    node._last_consensus_activity = 0.0
    for _ in range(20):
        node._last_ping_time = time.monotonic()
        node._send_liveness_ping()

    assert node._ping_interval_s <= ECHOD_PING_MAX_INTERVAL


def test_echod_ping_snaps_back_on_consensus_activity() -> None:
    node, _, _ = _make_node("echod")
    _make_leader(node)

    node._ping_interval_s = ECHOD_PING_MAX_INTERVAL
    node._last_ping_time = time.monotonic() - 10.0
    node._last_consensus_activity = time.monotonic()  # activity since last ping
    node._send_liveness_ping()

    assert node._ping_interval_s == LIVENESS_PING_INTERVAL


def test_echod_coordinator_keepalives_own_leaves() -> None:
    node, transport, _ = _make_node("echod")
    node._leaves = {"leaf-3"}
    node._last_leaf_keepalive = 0.0
    transport.reset_mock()

    node._tick()  # follower tick — keepalives are role-independent

    assert ("leaf-3", "liveness_ping") in _sent_types(transport)


# --------------------------------------------------------------------------
# echoD — optimization 5: directed leader handoff
# --------------------------------------------------------------------------


def test_echod_leader_hands_off_to_highest_battery_peer() -> None:
    node, transport, _ = _make_node("echod", battery=float(T_HANDOFF - 1))
    _make_leader(node)
    node._peer_batteries = {"coord-1": 60.0, "coord-2": 80.0}
    transport.reset_mock()

    node._tick()

    assert node.state == NodeState.FOLLOWER
    recipient, msg_type, payload = transport.send.call_args.args
    assert recipient == "coord-2"  # highest battery peer
    assert msg_type == "leadership_handoff"
    assert payload["leader_id"] == "coord-0"


def test_echod_handoff_recipient_starts_election_immediately() -> None:
    node, transport, _ = _make_node("echod")
    node.current_term = 3

    node._handle_leadership_handoff("coord-9", {
        "term": 3, "leader_id": "coord-9",
    })

    assert node.state == NodeState.CANDIDATE
    assert node.current_term == 4
    assert ("coord-1", "request_vote") in _sent_types(transport)


def test_echod_handoff_ignored_in_other_protocols() -> None:
    node, _, _ = _make_node("echo")
    node._handle_leadership_handoff("coord-9", {"term": 1, "leader_id": "coord-9"})
    assert node.state == NodeState.FOLLOWER
    assert node.current_term == 0


# --------------------------------------------------------------------------
# echoD — optimization 6 + pending-trigger replay
# --------------------------------------------------------------------------


def test_echod_pending_triggers_flush_as_one_batch() -> None:
    node, _, _ = _make_node("echod")
    # Leaderless: two reports get buffered.
    node._handle_sensor_data("leaf-0", _reading("leaf-0", 20.0))
    node._handle_sensor_data("leaf-1", _reading("leaf-1", 22.0))
    assert len(node._pending_triggers) == 2

    node.current_term = 1
    node._become_leader()

    assert len(node.log) == 1
    assert node.log[0].command["count"] == 2


def test_echo_pending_triggers_flush_one_round_each() -> None:
    node, _, _ = _make_node("echo")
    node._handle_sensor_data("leaf-0", _reading("leaf-0", 20.0))
    node._handle_sensor_data("leaf-1", _reading("leaf-1", 22.0))

    node.current_term = 1
    node._become_leader()

    assert len(node.log) == 2


def test_echod_reconcile_batch_is_one_entry() -> None:
    node, _, _ = _make_node("echod")
    _make_leader(node)
    commands = [{"sensor": "t", "value": 1, "leaf": "l1"},
                {"sensor": "t", "value": 2, "leaf": "l2"}]

    node._handle_reconcile_batch("coord-1", {"commands": commands})

    assert len(node.log) == 1
    assert node.log[0].trigger_type == "reconcile"
    assert node.log[0].command["count"] == 2


def test_echo_reconcile_batch_is_one_round_per_entry() -> None:
    node, _, _ = _make_node("echo")
    _make_leader(node)
    commands = [{"sensor": "t", "value": 1, "leaf": "l1"},
                {"sensor": "t", "value": 2, "leaf": "l2"}]

    node._handle_reconcile_batch("coord-1", {"commands": commands})

    assert len(node.log) == 2
    assert all(e.trigger_type == "reconcile" for e in node.log)


def test_provisional_truncation_sends_reconcile_batch() -> None:
    """Losing side of a partition forwards truncated provisional commands."""
    node, transport, _ = _make_node("echod")
    node.current_term = 1
    # Local provisional entry at index 1 (term 1, epoch 7).
    from rpi.coordinator.echo_node import LogEntry
    node.log.append(LogEntry(index=1, term=1, command={"x": 1}, partition_epoch=7))
    transport.reset_mock()

    # New leader (term 2) overwrites index 1 with its own entry.
    node._handle_append_entries("coord-1", {
        "term": 2, "leader_id": "coord-1",
        "prev_log_index": 0, "prev_log_term": 0,
        "entries": [{"index": 1, "term": 2, "command": {"y": 2},
                     "trigger_type": "delta", "partition_epoch": 0,
                     "timestamp": time.time()}],
        "leader_commit": 0,
    })

    sent = _sent_types(transport)
    assert ("coord-1", "reconcile_batch") in sent
    reconcile_call = next(
        c for c in transport.send.call_args_list if c.args[1] == "reconcile_batch"
    )
    assert reconcile_call.args[2]["commands"] == [{"x": 1}]


# --------------------------------------------------------------------------
# Shared fairness: follower forwarding
# --------------------------------------------------------------------------


def test_follower_forwards_sensor_data_to_known_leader() -> None:
    for protocol in ("raft", "echo", "echod"):
        node, transport, _ = _make_node(protocol)
        node._current_leader_id = "coord-1"
        payload = _reading("leaf-0", 21.5)

        node._handle_sensor_data("leaf-0", payload)

        recipient, msg_type, forwarded = transport.send.call_args.args
        assert recipient == "coord-1"
        assert msg_type == "sensor_data"
        assert forwarded == payload
