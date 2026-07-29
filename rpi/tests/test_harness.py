"""Tests for the Phase 2 experiment harness.

Covers node instrumentation (per-type TX/RX counters, commit-latency
tracking), the partition test hook, reconcile deduplication, mock-leaf
workload determinism, and collector aggregation semantics.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock

from rpi.coordinator.battery import BatteryMonitor
from rpi.coordinator.echo_node import EchoCoordinator, NodeState
from rpi.metrics_collector import METRICS_FIELDNAMES, MetricsCollector
from rpi.mock_leaf import MockLeaf


def _make_node(
    protocol: str = "echod",
    peers: tuple[str, ...] = ("coord-1", "coord-2"),
    battery: float = 100.0,
    node_id: str = "coord-0",
) -> tuple[EchoCoordinator, MagicMock, BatteryMonitor]:
    transport = MagicMock()
    batt = BatteryMonitor(mock=True, initial_level=battery)
    batt.set_mock_drain_rate(0.0)
    node = EchoCoordinator(
        node_id=node_id, peers=list(peers),
        transport=transport, battery=batt, protocol=protocol,
    )
    return node, transport, batt


# --------------------------------------------------------------------------
# Per-type TX/RX counters
# --------------------------------------------------------------------------


def test_tx_counted_by_type() -> None:
    node, _, _ = _make_node("echo")
    node._start_election()
    assert node.messages_sent_by_type["request_vote"] == 2  # two peers
    assert node.total_messages_sent == 2


def test_rx_counted_by_type() -> None:
    node, _, _ = _make_node("echo")
    node._inbox = asyncio.Queue()
    node._inbox.put_nowait(("coord-1", "liveness_ping", {
        "term": 1, "leader_id": "coord-1",
    }))
    node._inbox.put_nowait(("coord-1", "liveness_ping", {
        "term": 1, "leader_id": "coord-1",
    }))
    node._drain_inbox()
    assert node.messages_recv_by_type["liveness_ping"] == 2
    assert node.total_messages_received == 2


# --------------------------------------------------------------------------
# Commit-latency tracking
# --------------------------------------------------------------------------


def test_commit_latency_recorded_on_majority() -> None:
    node, _, _ = _make_node("echod")
    node.current_term = 1
    node._become_leader()

    node._replicate_entry(command={"x": 1}, trigger="delta")
    # One follower ack (majority of 3 = self + 1) with match_index=1.
    node._handle_append_entries_response("coord-1", {
        "term": 1, "success": True, "responder_id": "coord-1",
        "match_index": 1, "responder_battery": 90,
    })

    assert node.commit_index == 1
    assert len(node._latencies) == 1
    assert node._latencies[0] >= 0.0


def test_peer_battery_tracked_from_append_response() -> None:
    node, _, _ = _make_node("echod")
    node.current_term = 1
    node._become_leader()
    node._handle_append_entries_response("coord-1", {
        "term": 1, "success": False, "responder_id": "coord-1",
        "match_index": 0, "responder_battery": 55,
    })
    assert node._peer_batteries["coord-1"] == 55.0


# --------------------------------------------------------------------------
# Partition test hook
# --------------------------------------------------------------------------


def test_partition_groups_compute_blocked_senders() -> None:
    node, _, _ = _make_node("echo", node_id="coord-0")
    node._handle_partition_control("partition-ctl", {
        "action": "partition",
        "groups": [["coord-0", "coord-1"], ["coord-2", "coord-3"]],
    })
    assert node._blocked_senders == {"coord-2", "coord-3"}
    assert node.partition_epoch == 1  # ECHO enters provisional mode


def test_partition_raft_blocks_but_no_provisional() -> None:
    node, _, _ = _make_node("raft", node_id="coord-0")
    node._handle_partition_control("partition-ctl", {
        "action": "partition",
        "groups": [["coord-0", "coord-1"], ["coord-2"]],
    })
    assert node._blocked_senders == {"coord-2"}
    assert node.partition_epoch == 0  # Raft has no provisional mode


def test_heal_restores_connectivity_and_epoch() -> None:
    node, _, _ = _make_node("echod", node_id="coord-0")
    node._handle_partition_control("partition-ctl", {
        "action": "partition",
        "groups": [["coord-0"], ["coord-1", "coord-2"]],
    })
    node._handle_partition_control("partition-ctl", {"action": "heal"})
    assert node._blocked_senders == set()
    assert node.partition_epoch == 0


def test_dispatch_drops_blocked_senders() -> None:
    node, _, _ = _make_node("echod")
    loop = MagicMock()
    node._loop = loop
    node._inbox = asyncio.Queue()
    node._blocked_senders = {"coord-1"}

    node._dispatch("coord-1", "liveness_ping", {"term": 1})
    loop.call_soon_threadsafe.assert_not_called()

    node._dispatch("coord-2", "liveness_ping", {"term": 1})
    loop.call_soon_threadsafe.assert_called_once()

    # partition_control itself is never blocked (heal must get through).
    node._blocked_senders = {"partition-ctl"}
    node._dispatch("partition-ctl", "partition_control", {"action": "heal"})
    assert loop.call_soon_threadsafe.call_count == 2


def test_local_leader_enters_provisional_as_local_leader() -> None:
    node, _, _ = _make_node("echod", node_id="coord-0")
    node.current_term = 1
    node._become_leader()
    node._handle_partition_control("partition-ctl", {
        "action": "partition",
        "groups": [["coord-0"], ["coord-1", "coord-2"]],
    })
    assert node.state == NodeState.LOCAL_LEADER
    assert node.partition_epoch == 1


# --------------------------------------------------------------------------
# Reconcile deduplication
# --------------------------------------------------------------------------


def test_reconcile_batch_is_idempotent() -> None:
    node, _, _ = _make_node("echod")
    node.current_term = 1
    node._become_leader()
    commands = [{"sensor": "t", "value": 1, "leaf": "l1"},
                {"sensor": "t", "value": 2, "leaf": "l2"}]

    node._handle_reconcile_batch("coord-1", {"commands": commands})
    node._handle_reconcile_batch("coord-2", {"commands": commands})

    assert len(node.log) == 1  # second batch fully deduplicated
    assert node.log[0].command["count"] == 2


# --------------------------------------------------------------------------
# Mock-leaf workload determinism
# --------------------------------------------------------------------------


def _make_leaf(seed: int, index: int, edge_filter: bool = True) -> MockLeaf:
    return MockLeaf(
        leaf_id=f"leaf-{index}", client=MagicMock(), cluster_id="c",
        coordinators=["coord-0"], edge_filter=edge_filter,
        workload="bursts", seed=seed, leaf_index=index,
    )


def test_workload_streams_are_deterministic_per_seed() -> None:
    a = _make_leaf(seed=42, index=0)
    b = _make_leaf(seed=42, index=0)
    seq_a = [a.next_burst_value() for _ in range(100)]
    seq_b = [b.next_burst_value() for _ in range(100)]
    assert seq_a == seq_b


def test_workload_streams_differ_across_leaves() -> None:
    a = _make_leaf(seed=42, index=0)
    b = _make_leaf(seed=42, index=1)
    seq_a = [a.next_burst_value() for _ in range(100)]
    seq_b = [b.next_burst_value() for _ in range(100)]
    assert seq_a != seq_b


def test_workload_breach_rate_matches_sim_profile() -> None:
    """~30% of steps must be large jumps (matching the sim's workload).

    The relative breach rate against DELTA_THRESHOLD depends on the
    random walk's wandering base value, so the stable invariant is the
    branch itself: ~30% of steps are jumps of >= 2.0 (drifts are <= 0.4).
    """
    leaf = _make_leaf(seed=42, index=0)
    jumps = 0
    prev = leaf._value
    for _ in range(2000):
        v = leaf.next_burst_value()
        if abs(v - prev) >= 2.0:
            jumps += 1
        prev = v
    rate = jumps / 2000
    assert 0.25 < rate < 0.35, f"jump rate {rate:.2%} out of expected band"


def test_edge_filter_suppresses_subthreshold_readings() -> None:
    leaf = _make_leaf(seed=42, index=0, edge_filter=True)
    leaf._registered = True
    leaf._coordinator_id = "coord-0"
    for _ in range(50):
        leaf.send_reading(leaf.next_burst_value())
    total = leaf.sent_count + leaf.suppressed_count
    assert total == 50
    assert leaf.suppressed_count > leaf.sent_count  # ~70% suppressed


# --------------------------------------------------------------------------
# Collector aggregation
# --------------------------------------------------------------------------


def _status(node: str, **kw) -> tuple[str, bytes]:
    payload = {
        "protocol": "echod", "state": "leader", "term": 1, "battery": 90,
        "messages_sent": 0, "messages_received": 0,
        "messages_sent_by_type": {}, "messages_recv_by_type": {},
        "consensus_rounds": 0, "avg_consensus_latency_ms": 0.0,
        "leader_changes": 0, **kw,
    }
    return f"echo/test-cluster/status/{node}", json.dumps(payload).encode()


def test_collector_aggregation_math() -> None:
    c = MetricsCollector(cluster_id="test-cluster")

    topic, payload = _status(
        "coord-0", state="leader",
        messages_sent=100, messages_received=40,
        messages_sent_by_type={"liveness_ping": 60, "append_entries": 40},
        messages_recv_by_type={"append_response": 38, "partition_control": 2},
        consensus_rounds=10, avg_consensus_latency_ms=5.0, leader_changes=1,
    )
    c.handle_message(topic, payload)
    topic, payload = _status(
        "coord-1", state="follower",
        messages_sent=50, messages_received=60,
        messages_recv_by_type={"liveness_ping": 60},
    )
    c.handle_message(topic, payload)
    # Leaf stats: 5 sensor sends, hears 10 keepalives.
    c.handle_message(
        "echo/test-cluster/stats/leaf-0",
        json.dumps({"leaf_id": "leaf-0", "sent": 5, "suppressed": 7,
                    "recv_by_type": {"liveness_ping": 10}}).encode(),
    )

    c.sample()  # leader present
    c.sample()  # (same statuses retained)
    row = c.summary("echod")

    # RX deliveries excluding plumbing: 38 + 60 + 10 = 108.
    assert row["total_messages"] == 108
    assert row["consensus_rounds"] == 10
    assert row["avg_consensus_latency_ms"] == 5.0
    assert row["leader_changes"] == 1
    assert row["availability_pct"] == 100.0
    assert row["messages_recv_by_type"] == {
        "append_response": 38, "liveness_ping": 70,
    }


def test_metrics_fieldnames_match_sim_reporter() -> None:
    """metrics.csv must be schema-identical to simulation results."""
    assert METRICS_FIELDNAMES == [
        "protocol", "total_messages", "consensus_rounds",
        "avg_consensus_latency_ms", "avg_messages_per_round",
        "leader_changes", "availability_pct",
    ]
