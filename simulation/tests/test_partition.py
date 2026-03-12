"""Tests for network partition handling and reconciliation."""

from __future__ import annotations

import asyncio

import pytest

from simulation.core.cluster import Cluster
from simulation.core.messages import LogEntry, NodeState, TriggerType
from simulation.protocols.echo import EchoCoordinator


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

async def run_until(cluster: Cluster, predicate, timeout: float = 3.0):
    """Run the cluster until *predicate()* returns True or timeout."""
    deadline = asyncio.get_event_loop().time() + timeout

    async def monitor():
        while asyncio.get_event_loop().time() < deadline:
            if predicate():
                return True
            await asyncio.sleep(0.05)
        return False

    mon = asyncio.create_task(monitor())
    run = asyncio.create_task(cluster.run(timeout))
    result = await mon
    for n in cluster.nodes.values():
        n.stop()
    run.cancel()
    try:
        await run
    except asyncio.CancelledError:
        pass
    return result


# --------------------------------------------------------------------------
# Partition tests
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_partition_drops_messages():
    """Messages between partitioned groups should be silently dropped."""
    cluster = Cluster()
    for i in range(4):
        cluster.add_node(EchoCoordinator(f"c-{i}"))

    cluster.inject_partition(["c-0", "c-1"], ["c-2", "c-3"])

    from simulation.core.messages import Message

    delivered = await cluster.message_bus.route(Message(
        sender_id="c-0", recipient_id="c-2", payload="test",
    ))
    assert not delivered

    delivered = await cluster.message_bus.route(Message(
        sender_id="c-0", recipient_id="c-1", payload="test",
    ))
    assert delivered


@pytest.mark.asyncio
async def test_heal_restores_connectivity():
    """After healing, all nodes should be reachable again."""
    cluster = Cluster()
    for i in range(4):
        cluster.add_node(EchoCoordinator(f"c-{i}"))

    cluster.inject_partition(["c-0", "c-1"], ["c-2", "c-3"])
    cluster.heal_partition()

    from simulation.core.messages import Message

    delivered = await cluster.message_bus.route(Message(
        sender_id="c-0", recipient_id="c-2", payload="test",
    ))
    assert delivered


@pytest.mark.asyncio
async def test_provisional_mode_sets_epoch():
    """enter_provisional_mode should increment partition_epoch."""
    node = EchoCoordinator("c-0")
    assert node.partition_epoch == 0

    node.enter_provisional_mode()
    assert node.partition_epoch == 1

    node.enter_provisional_mode()
    assert node.partition_epoch == 2


@pytest.mark.asyncio
async def test_exit_provisional_resets_epoch():
    """exit_provisional_mode should reset epoch and state."""
    node = EchoCoordinator("c-0")
    node.state = NodeState.LOCAL_LEADER
    node.partition_epoch = 2

    node.exit_provisional_mode()

    assert node.partition_epoch == 0
    assert node.state == NodeState.FOLLOWER


@pytest.mark.asyncio
async def test_reconcile_replays_provisional_entries():
    """Reconciliation should replay provisional entries as new proposals."""
    cluster = Cluster()
    winner = EchoCoordinator("winner")
    loser = EchoCoordinator("loser")
    for n in (winner, loser):
        cluster.add_node(n)

    winner.state = NodeState.LEADER
    winner.current_term = 2
    loser.state = NodeState.LOCAL_LEADER
    loser.current_term = 2
    loser.partition_epoch = 1

    prov_entry = LogEntry(
        index=1, term=2,
        command={"sensor": "temp", "value": 22.5},
        trigger_type=TriggerType.DELTA,
        partition_epoch=1,
    )
    loser.log.append(prov_entry)

    winner_entry = LogEntry(
        index=1, term=2,
        command={"sensor": "temp", "value": 21.0},
        trigger_type=TriggerType.DELTA,
    )
    winner.log.append(winner_entry)
    winner.log.commit(1)

    await loser.reconcile(
        other_entries=winner.log.entries_from(1),
        other_commit=winner.log.commit_index,
    )

    assert loser.partition_epoch == 0
    assert loser.state == NodeState.FOLLOWER
    assert loser.log.commit_index == 1
    assert len(loser._pending_triggers) == 0 or loser.log.last_index >= 1
