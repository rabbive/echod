"""Shared fixtures for the ECHO / Raft test suite."""

from __future__ import annotations

import pytest

from simulation.core.cluster import Cluster
from simulation.core.coordinator import CoordinatorNode
from simulation.core.leaf import LeafNode
from simulation.protocols.echo import EchoCoordinator, EchoLeaf
from simulation.protocols.raft import RaftNode


@pytest.fixture
def echo_cluster() -> Cluster:
    """3-coordinator, 2-leaf ECHO cluster (not yet running)."""
    cluster = Cluster()
    for i in range(3):
        cluster.add_node(EchoCoordinator(f"coord-{i}", battery=0.8))
    for i in range(2):
        cluster.add_node(EchoLeaf(f"leaf-{i}"))
    return cluster


@pytest.fixture
def raft_cluster() -> Cluster:
    """3-node Raft cluster (not yet running)."""
    cluster = Cluster()
    for i in range(3):
        cluster.add_node(RaftNode(f"raft-{i}"))
    return cluster
