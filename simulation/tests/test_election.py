"""Tests for leader election — both Raft and ECHO variants."""

from __future__ import annotations

import asyncio

import pytest

from simulation.core.cluster import Cluster
from simulation.core.messages import NodeState
from simulation.protocols.echo import EchoCoordinator
from simulation.protocols.raft import RaftNode


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

async def wait_for_leader(cluster: Cluster, timeout: float = 3.0) -> str | None:
    """Run the cluster until a leader emerges or *timeout* elapses."""
    deadline = asyncio.get_event_loop().time() + timeout

    async def monitor() -> str | None:
        while asyncio.get_event_loop().time() < deadline:
            for nid, node in cluster.nodes.items():
                if getattr(node, "state", None) == NodeState.LEADER:
                    return nid
            await asyncio.sleep(0.05)
        return None

    monitor_task = asyncio.create_task(monitor())
    cluster_task = asyncio.create_task(cluster.run(timeout))

    leader = await monitor_task
    for n in cluster.nodes.values():
        n.stop()
    cluster_task.cancel()
    try:
        await cluster_task
    except asyncio.CancelledError:
        pass
    return leader


# --------------------------------------------------------------------------
# Raft election tests
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_raft_elects_leader():
    """A 3-node Raft cluster must elect exactly one leader."""
    cluster = Cluster()
    for i in range(3):
        cluster.add_node(RaftNode(f"raft-{i}"))

    leader = await wait_for_leader(cluster)
    assert leader is not None

    leaders = [
        nid for nid, n in cluster.nodes.items()
        if getattr(n, "state", None) == NodeState.LEADER
    ]
    assert len(leaders) == 1


@pytest.mark.asyncio
async def test_raft_single_term_leader():
    """All nodes should agree on the same term as the leader."""
    cluster = Cluster()
    for i in range(5):
        cluster.add_node(RaftNode(f"raft-{i}"))

    leader_id = await wait_for_leader(cluster)
    assert leader_id is not None

    leader_node = cluster.nodes[leader_id]
    leader_term = leader_node.current_term

    for nid, node in cluster.nodes.items():
        assert node.current_term >= leader_term or nid == leader_id


# --------------------------------------------------------------------------
# ECHO election tests
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_echo_elects_leader():
    """A 3-coordinator ECHO cluster must elect exactly one leader."""
    cluster = Cluster()
    for i in range(3):
        cluster.add_node(EchoCoordinator(f"coord-{i}", battery=0.8))

    leader = await wait_for_leader(cluster)
    assert leader is not None

    leaders = [
        nid for nid, n in cluster.nodes.items()
        if getattr(n, "state", None) == NodeState.LEADER
    ]
    assert len(leaders) == 1


@pytest.mark.asyncio
async def test_echo_observer_cannot_win_election():
    """A coordinator in OBSERVER state must not become leader."""
    cluster = Cluster()
    for i in range(3):
        battery = 0.10 if i == 0 else 0.80
        node = EchoCoordinator(f"coord-{i}", battery=battery)
        if i == 0:
            node.state = NodeState.OBSERVER
        cluster.add_node(node)

    leader = await wait_for_leader(cluster)
    assert leader is not None
    assert leader != "coord-0", "OBSERVER node should not win election"


@pytest.mark.asyncio
async def test_echo_rejects_low_battery_candidate():
    """Voters should reject a RequestVote from a candidate below T_LOW."""
    cluster = Cluster()
    low = EchoCoordinator("low", battery=0.10)
    high1 = EchoCoordinator("high1", battery=0.90)
    high2 = EchoCoordinator("high2", battery=0.90)
    for n in (low, high1, high2):
        cluster.add_node(n)

    leader = await wait_for_leader(cluster)
    assert leader is not None
    assert leader != "low"


@pytest.mark.asyncio
async def test_echo_energy_weighted_scoring():
    """election_score() should weight votes by battery proportion."""
    node = EchoCoordinator("test", battery=0.80)
    node._votes_received = {"a": 90, "b": 80, "test": 80}
    score = node.election_score()
    expected = 3 * (80 / 90)
    assert abs(score - expected) < 0.01
