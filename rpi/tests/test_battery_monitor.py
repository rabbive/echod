"""Tests for mock battery demo APIs (Phase 2 RPi)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from rpi.coordinator.battery import BatteryMonitor
from rpi.coordinator.echo_node import EchoCoordinator


def test_set_mock_level_clamps() -> None:
    b = BatteryMonitor(mock=True, initial_level=50.0)
    b.set_mock_drain_rate(0.0)
    b.set_mock_level(-5)
    assert b.read_level() == 0
    b.set_mock_level(150)
    assert b.read_level() == 100


def test_set_mock_drain_paused_freezes_level() -> None:
    b = BatteryMonitor(mock=True, initial_level=80.0)
    b.set_mock_drain_rate(10.0)
    b.set_mock_drain_paused(True)
    assert b.read_level() == 80
    assert b.read_level() == 80


def test_demo_control_set_battery_updates_level() -> None:
    transport = MagicMock()
    battery = BatteryMonitor(mock=True, initial_level=100.0)
    battery.set_mock_drain_rate(0.0)
    node = EchoCoordinator(
        node_id="coord-0",
        peers=["coord-0", "coord-1"],
        transport=transport,
        battery=battery,
    )
    node._handle_demo_control("echo-dashboard", {"action": "set_battery", "level": 12})
    assert battery.read_level() == 12
    transport.publish_status.assert_called()


@pytest.mark.parametrize("action,extra", [
    ("set_drain_rate", {"rate": 0.1}),
    ("set_drain_paused", {"paused": True}),
])
def test_demo_control_drain_actions(action: str, extra: dict) -> None:
    transport = MagicMock()
    battery = BatteryMonitor(mock=True, initial_level=50.0)
    battery.set_mock_drain_rate(0.0)
    node = EchoCoordinator(
        node_id="coord-0",
        peers=["coord-1"],
        transport=transport,
        battery=battery,
    )
    payload = {"action": action, **extra}
    node._handle_demo_control("echo-dashboard", payload)
    transport.publish_status.assert_called()
