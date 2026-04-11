"""Tests for simulation.main helpers."""

from __future__ import annotations

import asyncio

import pytest

from simulation.core.messages import NodeState
from simulation.main import run_scenario
from simulation.metrics.collector import MetricsCollector


class _DummyNode:
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self.battery = 1.0
        self.state = NodeState.FOLLOWER

    def tick_battery(self, delta: float) -> None:
        self.battery = max(0.0, self.battery - delta)


class _DummyBus:
    def __init__(self) -> None:
        self.on_message = None
        self._partitions: list[set[str]] = []


class _DummyCluster:
    def __init__(self) -> None:
        self.nodes = {
            "n1": _DummyNode("n1"),
            "n2": _DummyNode("n2"),
        }
        self.message_bus = _DummyBus()
        self.inject_calls = 0
        self.heal_calls = 0

    def tick_all_batteries(self, delta: float) -> None:
        for node in self.nodes.values():
            node.tick_battery(delta)

    def inject_partition(self, group_a: list[str], group_b: list[str]) -> None:
        self.inject_calls += 1
        self.message_bus._partitions = [set(group_a), set(group_b)]

    def heal_partition(self) -> None:
        self.heal_calls += 1
        self.message_bus._partitions = []

    async def run(self, duration_seconds: float) -> None:
        await asyncio.sleep(duration_seconds)


@pytest.mark.asyncio
async def test_partition_at_zero_is_applied() -> None:
    cluster = _DummyCluster()
    collector = MetricsCollector()

    await run_scenario(
        cluster=cluster,
        collector=collector,
        duration=0.25,
        battery_drain=0.0,
        partition_at=0.0,
        heal_at=None,
    )

    assert cluster.inject_calls == 1


@pytest.mark.asyncio
async def test_heal_at_zero_is_applied_after_partition() -> None:
    cluster = _DummyCluster()
    collector = MetricsCollector()

    await run_scenario(
        cluster=cluster,
        collector=collector,
        duration=0.25,
        battery_drain=0.0,
        partition_at=0.0,
        heal_at=0.0,
    )

    assert cluster.inject_calls == 1
    assert cluster.heal_calls == 1
