"""Regression tests for the CLI runner helpers."""

from __future__ import annotations

import pytest

from simulation.main import run_scenario
from simulation.metrics.collector import MetricsCollector
from simulation.protocols.raft import build_raft_cluster


@pytest.mark.asyncio
async def test_partition_at_zero_injects_partition() -> None:
    """partition_at=0.0 should be treated as a valid time (not falsy)."""
    cluster = build_raft_cluster(node_count=4)
    collector = MetricsCollector()

    await run_scenario(
        cluster=cluster,
        collector=collector,
        duration=0.25,
        battery_drain=0.0,
        partition_at=0.0,
        heal_at=None,
    )

    # Partition should have been injected and not healed.
    assert cluster.message_bus._partitions

