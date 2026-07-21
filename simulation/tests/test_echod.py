"""Tests for the echoD hybrid protocol optimizations.

Covers: edge-side delta filtering, batched consensus, coordinators-only
adaptive liveness, battery-ordered election timeouts, directed leader
handoff, and batched reconciliation.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from simulation.core.cluster import Cluster
from simulation.core.config import (
    DELTA_THRESHOLD,
    ECHOD_ELECTION_TIMEOUT_MIN,
    ECHOD_PING_MAX_INTERVAL,
    LIVENESS_PING_INTERVAL,
    T_HANDOFF,
)
from simulation.core.messages import (
    LeadershipHandoff,
    LivenessPing,
    NodeState,
    RequestVoteRPC,
    SensorDataReport,
    TriggerType,
)
from simulation.protocols.echod import (
    EchoDCoordinator,
    EchoDLeaf,
    build_echod_cluster,
)
from simulation.tests.test_election import wait_for_leader


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def _wire(cluster: Cluster, *nodes) -> None:
    for n in nodes:
        cluster.add_node(n)


def _make_leader(node: EchoDCoordinator) -> None:
    node.state = NodeState.LEADER
    node.current_term = 1


# --------------------------------------------------------------------------
# 1. Edge-side delta filtering
# --------------------------------------------------------------------------

class TestEdgeFiltering:
    @pytest.mark.asyncio
    async def test_subthreshold_reading_is_suppressed(self):
        cluster = Cluster()
        coord = EchoDCoordinator("coord-0")
        leaf = EchoDLeaf("leaf-0", auto_report=False)
        _wire(cluster, coord, leaf)
        leaf.coordinator_id = "coord-0"

        await leaf.inject_reading(20.0)          # first reading — sent
        assert cluster.message_bus.total_messages == 1

        # +1 % change — below the 5 % threshold — must not be sent
        await leaf.inject_reading(20.2)
        assert cluster.message_bus.total_messages == 1
        assert leaf.suppressed_count == 1

    @pytest.mark.asyncio
    async def test_threshold_breach_is_transmitted(self):
        cluster = Cluster()
        coord = EchoDCoordinator("coord-0")
        leaf = EchoDLeaf("leaf-0", auto_report=False)
        _wire(cluster, coord, leaf)
        leaf.coordinator_id = "coord-0"

        await leaf.inject_reading(20.0)
        await leaf.inject_reading(22.0)          # +10 % — breach
        assert cluster.message_bus.total_messages == 2

    @pytest.mark.asyncio
    async def test_filter_tracks_last_transmitted_not_last_read(self):
        """Suppressed readings must not move the reference value."""
        leaf = EchoDLeaf("leaf-0")
        leaf.coordinator_id = "coord-0"
        await leaf.inject_reading(20.0)
        assert leaf._last_transmitted == 20.0

        # Suppressed reading — reference stays at 20.0
        await leaf.inject_reading(20.1)
        assert leaf._last_transmitted == 20.0


# --------------------------------------------------------------------------
# 2. Batched event-driven consensus
# --------------------------------------------------------------------------

class TestBatching:
    @pytest.mark.asyncio
    async def test_burst_becomes_single_log_entry(self):
        cluster = Cluster()
        leader = EchoDCoordinator("coord-0")
        peers = [EchoDCoordinator(f"coord-{i}") for i in (1, 2)]
        _wire(cluster, leader, *peers)
        _make_leader(leader)

        for i in range(3):
            await leader.handle_sensor_data("leaf-x", SensorDataReport(
                leaf_id=f"leaf-{i}", sensor_type="temp", value=20.0 + i * 10,
            ))

        assert len(leader._batch_buffer) == 3
        await leader._flush_batch()

        assert leader.log.last_index == 1          # ONE entry, not three
        entry = leader.log.get(1)
        assert entry.command["count"] == 3
        assert len(entry.command["batch"]) == 3

    @pytest.mark.asyncio
    async def test_single_reading_not_wrapped_in_batch(self):
        cluster = Cluster()
        leader = EchoDCoordinator("coord-0")
        _wire(cluster, leader)
        _make_leader(leader)

        await leader.handle_sensor_data("leaf-0", SensorDataReport(
            leaf_id="leaf-0", sensor_type="temp", value=25.0,
        ))
        await leader._flush_batch()

        entry = leader.log.get(1)
        assert "batch" not in entry.command
        assert entry.command["value"] == 25.0

    @pytest.mark.asyncio
    async def test_coordinator_delta_backstop_still_applies(self):
        """Sub-threshold reports reaching the leader are dropped, not batched."""
        cluster = Cluster()
        leader = EchoDCoordinator("coord-0")
        _wire(cluster, leader)
        _make_leader(leader)

        await leader.handle_sensor_data("leaf-0", SensorDataReport(
            leaf_id="leaf-0", sensor_type="temp", value=20.0,
        ))
        await leader.handle_sensor_data("leaf-0", SensorDataReport(
            leaf_id="leaf-0", sensor_type="temp", value=20.1,   # +0.5 %
        ))
        assert len(leader._batch_buffer) == 1

    @pytest.mark.asyncio
    async def test_full_batch_flushes_immediately(self):
        from simulation.core.config import MAX_BATCH_SIZE

        cluster = Cluster()
        leader = EchoDCoordinator("coord-0")
        _wire(cluster, leader)
        _make_leader(leader)

        for i in range(MAX_BATCH_SIZE):
            await leader.handle_sensor_data("leaf-x", SensorDataReport(
                leaf_id=f"leaf-{i}", sensor_type="temp", value=10.0 * (i + 1),
            ))

        assert leader.log.last_index == 1          # flushed without waiting
        assert len(leader._batch_buffer) == 0


# --------------------------------------------------------------------------
# 3. Coordinators-only adaptive liveness
# --------------------------------------------------------------------------

class TestAdaptiveLiveness:
    @pytest.mark.asyncio
    async def test_pings_go_to_coordinators_only(self):
        cluster = Cluster()
        leader = EchoDCoordinator("coord-0")
        peer = EchoDCoordinator("coord-1")
        leaves = [EchoDLeaf(f"leaf-{i}") for i in range(3)]
        _wire(cluster, leader, peer, *leaves)
        _make_leader(leader)

        await leader._send_liveness_ping()

        assert cluster.message_bus.total_messages == 1   # only coord-1
        assert peer._inbox.qsize() == 1
        for leaf in leaves:
            assert leaf._inbox.qsize() == 0

    @pytest.mark.asyncio
    async def test_interval_backs_off_when_idle(self):
        cluster = Cluster()
        leader = EchoDCoordinator("coord-0")
        _wire(cluster, leader)
        _make_leader(leader)
        leader._last_consensus_activity = time.monotonic() - 100  # long ago
        leader._last_ping_time = time.monotonic()               # just pinged

        intervals = []
        for _ in range(4):
            await leader._send_liveness_ping()
            intervals.append(leader._ping_interval_ms)
            leader._last_ping_time = time.monotonic()

        assert intervals[0] > LIVENESS_PING_INTERVAL      # growing
        assert intervals[-1] <= ECHOD_PING_MAX_INTERVAL   # capped
        assert intervals == sorted(intervals)             # monotonic

    @pytest.mark.asyncio
    async def test_interval_snaps_back_on_activity(self):
        cluster = Cluster()
        leader = EchoDCoordinator("coord-0")
        _wire(cluster, leader)
        _make_leader(leader)

        leader._ping_interval_ms = float(ECHOD_PING_MAX_INTERVAL)
        leader._last_consensus_activity = time.monotonic()  # just happened
        leader._last_ping_time = time.monotonic() - 10      # long before

        await leader._send_liveness_ping()
        assert leader._ping_interval_ms == LIVENESS_PING_INTERVAL

    @pytest.mark.asyncio
    async def test_leaf_keepalives_from_any_coordinator(self):
        """Non-leader coordinators keep their own leaves alive."""
        cluster = Cluster()
        coord = EchoDCoordinator("coord-0")   # FOLLOWER, not leader
        leaf = EchoDLeaf("leaf-0")
        _wire(cluster, coord, leaf)
        coord._leaves.add("leaf-0")

        await coord._send_leaf_keepalives()

        assert cluster.message_bus.total_messages == 1
        msg = await asyncio.wait_for(leaf._inbox.get(), timeout=0.1)
        assert isinstance(msg.payload, LivenessPing)


# --------------------------------------------------------------------------
# 4. Battery-ordered election timeouts
# --------------------------------------------------------------------------

class TestBatteryOrderedTimeouts:
    def test_higher_battery_times_out_first(self):
        high = EchoDCoordinator("coord-high", battery=1.0)
        low = EchoDCoordinator("coord-low", battery=0.5)

        now = time.monotonic()
        high_deadline = high._new_election_deadline() - now
        low_deadline = low._new_election_deadline() - now

        # Low battery adds (1 - 0.5) * 300 ms = 150 ms of delay; the
        # tie-break (< 30 ms) can never overcome that.
        assert high_deadline < low_deadline

    def test_timeout_respects_minimum(self):
        node = EchoDCoordinator("coord-0", battery=1.0)
        timeout_ms = (node._new_election_deadline() - time.monotonic()) * 1000
        assert timeout_ms >= ECHOD_ELECTION_TIMEOUT_MIN

    @pytest.mark.asyncio
    async def test_highest_battery_node_wins_election(self):
        cluster = Cluster()
        batteries = {"coord-0": 0.5, "coord-1": 1.0, "coord-2": 0.5}
        for nid, bat in batteries.items():
            cluster.add_node(EchoDCoordinator(nid, battery=bat))

        leader = await wait_for_leader(cluster)
        assert leader == "coord-1"

    @pytest.mark.asyncio
    async def test_request_vote_excludes_leaves(self):
        cluster = Cluster()
        candidate = EchoDCoordinator("coord-0")
        peer = EchoDCoordinator("coord-1")
        leaf = EchoDLeaf("leaf-0")
        _wire(cluster, candidate, peer, leaf)

        await candidate.start_election()

        assert cluster.message_bus.total_messages == 1
        msg = await asyncio.wait_for(peer._inbox.get(), timeout=0.1)
        assert isinstance(msg.payload, RequestVoteRPC)
        assert leaf._inbox.qsize() == 0


# --------------------------------------------------------------------------
# 5. Directed leader handoff
# --------------------------------------------------------------------------

class TestHandoff:
    @pytest.mark.asyncio
    async def test_handoff_nominates_highest_battery_peer(self):
        cluster = Cluster()
        leader = EchoDCoordinator("coord-0", battery=(T_HANDOFF - 1) / 100)
        peer_a = EchoDCoordinator("coord-1", battery=0.9)
        peer_b = EchoDCoordinator("coord-2", battery=0.8)
        _wire(cluster, leader, peer_a, peer_b)
        _make_leader(leader)
        leader._peer_batteries = {"coord-1": 90, "coord-2": 80}

        await leader._initiate_handoff()

        assert leader.state == NodeState.FOLLOWER
        assert cluster.message_bus.total_messages == 1
        msg = await asyncio.wait_for(peer_a._inbox.get(), timeout=0.1)
        assert isinstance(msg.payload, LeadershipHandoff)
        assert peer_b._inbox.qsize() == 0

    @pytest.mark.asyncio
    async def test_no_handoff_when_no_better_candidate(self):
        cluster = Cluster()
        leader = EchoDCoordinator("coord-0", battery=(T_HANDOFF - 1) / 100)
        peer = EchoDCoordinator("coord-1", battery=0.5)
        _wire(cluster, leader, peer)
        _make_leader(leader)
        leader._peer_batteries = {"coord-1": 10}   # worse than leader

        await leader._initiate_handoff()

        assert leader.state == NodeState.LEADER    # stayed
        assert cluster.message_bus.total_messages == 0

    @pytest.mark.asyncio
    async def test_nominated_successor_starts_election(self):
        successor = EchoDCoordinator("coord-1", battery=0.9)
        successor.cluster = Cluster()
        successor.cluster.add_node(successor)

        await successor.handle_leadership_handoff("coord-0", LeadershipHandoff(
            term=1, leader_id="coord-0",
        ))

        assert successor.state == NodeState.CANDIDATE
        assert successor.voted_for == "coord-1"

    @pytest.mark.asyncio
    async def test_handoff_transfers_leadership_end_to_end(self):
        """A draining leader hands off; the cluster keeps a leader."""
        cluster = Cluster()
        coords = [EchoDCoordinator(f"coord-{i}", battery=1.0) for i in range(3)]
        for c in coords:
            cluster.add_node(c)

        run_task = asyncio.create_task(cluster.run(8.0))
        try:
            async def current_leader() -> str | None:
                for nid, node in cluster.nodes.items():
                    if node.state == NodeState.LEADER:
                        return nid
                return None

            # Wait for the initial leader.
            leader_id = None
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline and leader_id is None:
                leader_id = await current_leader()
                if leader_id is None:
                    await asyncio.sleep(0.05)
            assert leader_id is not None

            # Drain the leader below T_HANDOFF and let its idle loop react.
            cluster.nodes[leader_id].battery = (T_HANDOFF - 5) / 100

            # Leadership should move to another node via handoff.
            new_leader = None
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline and new_leader is None:
                nid = await current_leader()
                if nid is not None and nid != leader_id:
                    new_leader = nid
                else:
                    await asyncio.sleep(0.05)

            assert new_leader is not None
            assert new_leader != leader_id
        finally:
            for n in cluster.nodes.values():
                n.stop()
            run_task.cancel()
            try:
                await run_task
            except asyncio.CancelledError:
                pass


# --------------------------------------------------------------------------
# 6. Batched reconciliation / pending-trigger replay
# --------------------------------------------------------------------------

class TestBatchedReconciliation:
    @pytest.mark.asyncio
    async def test_pending_triggers_replay_as_one_entry(self):
        cluster = Cluster()
        leader = EchoDCoordinator("coord-0")
        _wire(cluster, leader)
        _make_leader(leader)

        leader._pending_triggers = [
            ({"value": 1}, TriggerType.DELTA),
            ({"value": 2}, TriggerType.DELTA),
            ({"value": 3}, TriggerType.RECONCILE),
        ]
        await leader._flush_pending_triggers()

        assert leader.log.last_index == 1
        entry = leader.log.get(1)
        assert entry.command["count"] == 3
        assert entry.trigger_type == TriggerType.DELTA

    @pytest.mark.asyncio
    async def test_single_pending_trigger_replays_plain(self):
        cluster = Cluster()
        leader = EchoDCoordinator("coord-0")
        _wire(cluster, leader)
        _make_leader(leader)

        leader._pending_triggers = [({"value": 42}, TriggerType.RECONCILE)]
        await leader._flush_pending_triggers()

        entry = leader.log.get(1)
        assert entry.command == {"value": 42}
        assert entry.trigger_type == TriggerType.RECONCILE


# --------------------------------------------------------------------------
# Cluster builder
# --------------------------------------------------------------------------

class TestBuilder:
    def test_build_echod_cluster_topology(self):
        cluster = build_echod_cluster(coordinator_count=3, leaf_count=4)
        coords = [n for n in cluster.nodes.values() if n.tier == "coordinator"]
        leaves = [n for n in cluster.nodes.values() if n.tier == "leaf"]
        assert len(coords) == 3
        assert len(leaves) == 4
        assert all(isinstance(n, EchoDCoordinator) for n in coords)
        assert all(isinstance(n, EchoDLeaf) for n in leaves)

    @pytest.mark.asyncio
    async def test_echod_cluster_elects_single_leader(self):
        cluster = build_echod_cluster(coordinator_count=5, leaf_count=0)
        leader = await wait_for_leader(cluster)
        assert leader is not None
        leaders = [
            nid for nid, n in cluster.nodes.items()
            if n.state == NodeState.LEADER
        ]
        assert len(leaders) == 1
