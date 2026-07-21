"""Deterministic workload generator for fair protocol comparison.

Produces a seeded schedule of sensor-reading events that is replayed
identically for Raft, ECHO, and echoD, so any difference in messages,
latency, or energy comes from the protocols — not the traffic.

Event model: once per burst interval, every source emits one reading at
the same instant (synchronised sensing, typical for IoT deployments).
Values follow a per-source random walk of mostly small drifts with
occasional large jumps, so roughly a third of the readings breach the
delta threshold — exercising the filtering paths of ECHO/echoD while
vanilla Raft replicates every event it is given.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import TYPE_CHECKING

from simulation.core.messages import NodeState

if TYPE_CHECKING:
    from simulation.core.cluster import Cluster


@dataclass(frozen=True)
class WorkloadEvent:
    """A single sensor reading to inject at a fixed simulation time."""
    time_s: float
    source_index: int     # leaf index (logical source id for Raft)
    value: float


def generate_workload(
    duration: float,
    leaf_count: int,
    burst_interval: float = 1.0,
    seed: int = 42,
    start_at: float = 1.0,
) -> list[WorkloadEvent]:
    """Build the event schedule.  Identical inputs → identical schedule."""
    rng = random.Random(seed)
    events: list[WorkloadEvent] = []
    values = [20.0 + rng.uniform(-2, 2) for _ in range(max(leaf_count, 1))]

    t = start_at
    while t <= duration:
        for i in range(len(values)):
            if rng.random() < 0.3:
                # Large jump (10–25 % of ~20) — breaches DELTA_THRESHOLD.
                step = rng.choice((-1.0, 1.0)) * rng.uniform(2.0, 5.0)
            else:
                # Small drift (< 2 %) — sub-threshold, filterable.
                step = rng.uniform(-0.4, 0.4)
            values[i] += step
            events.append(WorkloadEvent(time_s=t, source_index=i, value=values[i]))
        t += burst_interval

    return events


async def deliver_event(cluster: Cluster, event: WorkloadEvent) -> None:
    """Deliver one workload event through the protocol's normal path.

    - Tiered protocols (ECHO/echoD): the reading is injected into the
      corresponding leaf, which reports it to its coordinator.
    - Flat protocols (Raft): the reading is proposed directly to the
      current leader as a client command.
    """
    leaves = sorted(
        (n for n in cluster.nodes.values() if n.tier == "leaf"),
        key=lambda n: n.node_id,
    )
    if leaves:
        leaf = leaves[event.source_index % len(leaves)]
        await leaf.inject_reading(event.value)
        return

    leader = next(
        (
            n for n in cluster.nodes.values()
            if getattr(n, "state", None) == NodeState.LEADER
        ),
        None,
    )
    if leader is not None and hasattr(leader, "replicate"):
        await leader.replicate({
            "sensor": "workload",
            "src": event.source_index,
            "value": event.value,
        })
