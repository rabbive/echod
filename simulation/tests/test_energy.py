"""Tests for battery management and energy-based state transitions."""

from __future__ import annotations

import pytest

from simulation.core.config import T_LOW, T_RESTORE
from simulation.core.messages import NodeState
from simulation.protocols.echo import EchoCoordinator


class TestBatteryDrain:
    """tick_battery should correctly drain and trigger state transitions."""

    def test_drain_reduces_battery(self):
        node = EchoCoordinator("c-0", battery=1.0)
        node.tick_battery(0.1)
        assert abs(node.battery - 0.9) < 1e-9

    def test_battery_does_not_go_negative(self):
        node = EchoCoordinator("c-0", battery=0.05)
        node.tick_battery(0.1)
        assert node.battery == 0.0

    def test_battery_capped_at_one(self):
        node = EchoCoordinator("c-0", battery=0.95)
        node.tick_battery(-0.1)  # negative drain = charge
        assert node.battery == 1.0


class TestObserverTransition:
    """Coordinators must become OBSERVER at T_LOW and recover at T_RESTORE."""

    def test_becomes_observer_below_t_low(self):
        node = EchoCoordinator("c-0", battery=T_LOW / 100 + 0.001)
        node.state = NodeState.FOLLOWER

        # Drain just past T_LOW
        node.tick_battery(0.002)
        assert node.state == NodeState.OBSERVER

    def test_stays_observer_between_t_low_and_t_restore(self):
        node = EchoCoordinator("c-0", battery=0.20)
        node.state = NodeState.OBSERVER

        # 20% is between T_LOW (15%) and T_RESTORE (25%) — stay observer
        node.tick_battery(0.0)
        assert node.state == NodeState.OBSERVER

    def test_recovers_at_t_restore(self):
        node = EchoCoordinator("c-0", battery=T_RESTORE / 100)
        node.state = NodeState.OBSERVER

        # At exactly T_RESTORE (25%) the node should re-enter
        node.tick_battery(0.0)
        assert node.state == NodeState.FOLLOWER

    def test_leader_becomes_observer_on_low_battery(self):
        node = EchoCoordinator("c-0", battery=T_LOW / 100 + 0.001)
        node.state = NodeState.LEADER

        node.tick_battery(0.002)
        assert node.state == NodeState.OBSERVER

    def test_voted_for_cleared_on_observer_transition(self):
        node = EchoCoordinator("c-0", battery=T_LOW / 100 + 0.001)
        node.voted_for = "c-1"

        node.tick_battery(0.002)
        assert node.voted_for is None


class TestStepDown:
    """step_down should revert to follower on higher term."""

    def test_candidate_steps_down(self):
        node = EchoCoordinator("c-0")
        node.current_term = 1
        node.state = NodeState.CANDIDATE

        node.step_down(2)

        assert node.current_term == 2
        assert node.state == NodeState.FOLLOWER
        assert node.voted_for is None

    def test_leader_steps_down(self):
        node = EchoCoordinator("c-0")
        node.current_term = 3
        node.state = NodeState.LEADER

        node.step_down(5)

        assert node.current_term == 5
        assert node.state == NodeState.FOLLOWER

    def test_follower_updates_term(self):
        node = EchoCoordinator("c-0")
        node.current_term = 1
        node.state = NodeState.FOLLOWER

        node.step_down(4)

        assert node.current_term == 4
        assert node.state == NodeState.FOLLOWER
