"""Regression test for MQTT-thread -> asyncio-loop message dispatch."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from rpi.coordinator.battery import BatteryMonitor
from rpi.coordinator.echo_node import EchoCoordinator


@pytest.mark.asyncio
async def test_dispatch_enqueues_and_processes_in_event_loop() -> None:
    transport = MagicMock()
    battery = BatteryMonitor(mock=True, initial_level=100.0)
    battery.set_mock_drain_rate(0.0)

    node = EchoCoordinator(
        node_id="coord-0",
        peers=["coord-1"],
        transport=transport,
        battery=battery,
    )

    task = asyncio.create_task(node.run())
    try:
        # Give the node time to initialise its asyncio inbox.
        await asyncio.sleep(0.02)

        # Simulate inbound message arriving from MQTT thread.
        node._dispatch(
            sender="coord-1",
            msg_type="request_vote",
            payload={
                "term": 1,
                "candidate_id": "coord-1",
                "last_log_index": 0,
                "last_log_term": 0,
                "battery_level": 100,
            },
        )

        await asyncio.sleep(0.05)

        assert node.total_messages_received >= 1
        transport.send.assert_called()
    finally:
        node.stop()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

