"""CLI runner for the Raft vs ECHO vs echoD simulation.

Runs all protocols under identical conditions — the same seeded workload
schedule, the same number of consensus participants, and the same energy
model — and outputs comparative metrics to CSV (and optionally charts).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import sys
import time

from simulation.core.cluster import Cluster
from simulation.core.config import ENERGY_RX_COST, ENERGY_TX_COST
from simulation.core.messages import NodeState
from simulation.metrics.collector import MetricsCollector
from simulation.metrics.reporter import export_csv, export_energy_csv, generate_charts
from simulation.protocols.echo import build_echo_cluster
from simulation.protocols.echod import build_echod_cluster
from simulation.protocols.raft import build_raft_cluster
from simulation.workload import WorkloadEvent, deliver_event, generate_workload


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

LEADER_STATES = (NodeState.LEADER, NodeState.LOCAL_LEADER)


# ---------------------------------------------------------------------- run
async def run_scenario(
    cluster: Cluster,
    collector: MetricsCollector,
    duration: float,
    battery_drain: float,
    partition_at: float | None,
    heal_at: float | None,
    workload: list[WorkloadEvent] | None = None,
) -> None:
    """Run a single protocol scenario with optional partition injection.

    The energy model combines role-weighted idle drain (leader > candidate
    > follower > observer, leaves cheapest) with per-message TX/RX costs,
    so messaging efficiency shows up directly in the energy metrics.
    """
    def on_message(msg) -> None:
        collector.on_message(msg)
        src = cluster.nodes.get(msg.sender_id)
        dst = cluster.nodes.get(msg.recipient_id)
        if src is not None:
            src.tick_battery(ENERGY_TX_COST)
        if dst is not None:
            dst.tick_battery(ENERGY_RX_COST)

    cluster.message_bus.on_message = on_message

    for node in cluster.nodes.values():
        collector.snapshot_energy(node.node_id, node.battery)

    async def side_effects() -> None:
        start = time.monotonic()
        tick = 0.1  # seconds between housekeeping ticks
        last_leader: str | None = None
        while True:
            await asyncio.sleep(tick)
            elapsed = time.monotonic() - start

            cluster.tick_batteries_weighted(battery_drain * tick)

            has_leader = any(
                getattr(n, "state", None) in LEADER_STATES
                for n in cluster.nodes.values()
            )
            collector.record_availability(has_leader)

            for node in cluster.nodes.values():
                collector.snapshot_energy(node.node_id, node.battery)

            if (
                partition_at is not None
                and elapsed >= partition_at
                and cluster.message_bus._partitions == []
            ):
                ids = list(cluster.nodes.keys())
                mid = len(ids) // 2
                cluster.inject_partition(ids[:mid], ids[mid:])

            if (
                heal_at is not None
                and elapsed >= heal_at
                and cluster.message_bus._partitions
            ):
                cluster.heal_partition()

            leader_id = next(
                (
                    n.node_id
                    for n in cluster.nodes.values()
                    if getattr(n, "state", None) in LEADER_STATES
                ),
                None,
            )
            if leader_id != last_leader:
                if leader_id is not None:
                    collector.record_leader_change(leader_id)
                last_leader = leader_id

            if elapsed >= duration:
                break

    async def workload_driver() -> None:
        """Replay the seeded event schedule through the protocol's path."""
        if not workload:
            return
        start = time.monotonic()
        for event in workload:
            delay = event.time_s - (time.monotonic() - start)
            if delay > 0:
                await asyncio.sleep(delay)
            await deliver_event(cluster, event)

    effects_task = asyncio.create_task(side_effects())
    workload_task = asyncio.create_task(workload_driver())
    try:
        await cluster.run(duration)
    finally:
        for task in (effects_task, workload_task):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


# --------------------------------------------------------------------- CLI
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Raft vs ECHO vs echoD consensus simulation",
    )
    p.add_argument(
        "--coordinators", type=int, default=5,
        help="Number of coordinator / Raft peer nodes (default: 5)",
    )
    p.add_argument(
        "--leaves", type=int, default=10,
        help="Number of ECHO/echoD leaf nodes (default: 10)",
    )
    p.add_argument(
        "--duration", type=float, default=5.0,
        help="Simulation duration in seconds (default: 5)",
    )
    p.add_argument(
        "--battery-drain", type=float, default=0.01,
        help="Base idle drain rate per second (0–1 scale, default: 0.01)",
    )
    p.add_argument(
        "--partition-at", type=float, default=None,
        help="Inject partition at N seconds into the simulation",
    )
    p.add_argument(
        "--heal-at", type=float, default=None,
        help="Heal partition at N seconds",
    )
    p.add_argument(
        "--seed", type=int, default=42,
        help="Seed for the workload schedule and election timers (default: 42)",
    )
    p.add_argument(
        "--burst-interval", type=float, default=1.0,
        help="Seconds between workload bursts (default: 1.0)",
    )
    p.add_argument(
        "--no-workload", action="store_true",
        help="Disable the workload generator (idle-cluster comparison)",
    )
    p.add_argument(
        "--charts", action="store_true",
        help="Generate matplotlib comparison charts",
    )
    p.add_argument(
        "--output-dir", type=str, default="results",
        help="Directory for CSV / chart output (default: results)",
    )
    return p.parse_args(argv)


async def main(args: argparse.Namespace) -> None:
    collectors: dict[str, MetricsCollector] = {}

    # One seeded schedule, replayed identically for every protocol.
    random.seed(args.seed)
    workload = None
    if not args.no_workload:
        workload = generate_workload(
            duration=args.duration,
            leaf_count=args.leaves,
            burst_interval=args.burst_interval,
            seed=args.seed,
        )
        logger.info("Workload: %d events (seed=%d)", len(workload), args.seed)

    # --- Raft ---
    logger.info("=== Running Raft baseline ===")
    raft_cluster = build_raft_cluster(node_count=args.coordinators)
    raft_collector = MetricsCollector()
    await run_scenario(
        raft_cluster, raft_collector,
        duration=args.duration,
        battery_drain=args.battery_drain,
        partition_at=args.partition_at,
        heal_at=args.heal_at,
        workload=workload,
    )
    collectors["raft"] = raft_collector

    # --- ECHO ---
    logger.info("=== Running ECHO protocol ===")
    echo_cluster = build_echo_cluster(
        coordinator_count=args.coordinators,
        leaf_count=args.leaves,
        auto_report=False,
    )
    echo_collector = MetricsCollector()
    await run_scenario(
        echo_cluster, echo_collector,
        duration=args.duration,
        battery_drain=args.battery_drain,
        partition_at=args.partition_at,
        heal_at=args.heal_at,
        workload=workload,
    )
    collectors["echo"] = echo_collector

    # --- echoD ---
    logger.info("=== Running echoD hybrid ===")
    echod_cluster = build_echod_cluster(
        coordinator_count=args.coordinators,
        leaf_count=args.leaves,
        auto_report=False,
    )
    echod_collector = MetricsCollector()
    await run_scenario(
        echod_cluster, echod_collector,
        duration=args.duration,
        battery_drain=args.battery_drain,
        partition_at=args.partition_at,
        heal_at=args.heal_at,
        workload=workload,
    )
    collectors["echod"] = echod_collector

    # --- Output ---
    csv_path = export_csv(collectors, output_dir=args.output_dir)
    energy_path = export_energy_csv(collectors, output_dir=args.output_dir)
    logger.info("Metrics CSV: %s", csv_path)
    logger.info("Energy CSV:  %s", energy_path)

    if args.charts:
        chart_paths = generate_charts(collectors, output_dir=args.output_dir)
        for cp in chart_paths:
            logger.info("Chart: %s", cp)

    # Print summary to stdout
    for name, col in collectors.items():
        print(f"\n{'=' * 40}")
        print(f"  {name.upper()} Summary")
        print(f"{'=' * 40}")
        for k, v in col.summary().items():
            print(f"  {k}: {v}")


def cli() -> None:
    args = parse_args()
    asyncio.run(main(args))


if __name__ == "__main__":
    cli()
