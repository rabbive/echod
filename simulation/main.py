"""CLI runner for the ECHO vs Raft simulation.

Runs both protocols under identical conditions and outputs comparative
metrics to CSV (and optionally matplotlib charts).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time

from simulation.core.cluster import Cluster
from simulation.core.messages import NodeState
from simulation.metrics.collector import MetricsCollector
from simulation.metrics.reporter import export_csv, export_energy_csv, generate_charts
from simulation.protocols.echo import build_echo_cluster
from simulation.protocols.raft import build_raft_cluster


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------- run
async def run_scenario(
    cluster: Cluster,
    collector: MetricsCollector,
    duration: float,
    battery_drain: float,
    partition_at: float | None,
    heal_at: float | None,
) -> None:
    """Run a single protocol scenario with optional partition injection."""
    cluster.message_bus.on_message = collector.on_message
    partition_injected = False
    partition_healed = False

    for node in cluster.nodes.values():
        collector.snapshot_energy(node.node_id, node.battery)

    async def side_effects() -> None:
        nonlocal partition_injected, partition_healed
        start = time.monotonic()
        tick = 0.1  # seconds between housekeeping ticks
        while True:
            await asyncio.sleep(tick)
            elapsed = time.monotonic() - start

            cluster.tick_all_batteries(battery_drain * tick)

            has_leader = any(
                getattr(n, "state", None) == NodeState.LEADER
                for n in cluster.nodes.values()
            )
            collector.record_availability(has_leader)

            for node in cluster.nodes.values():
                collector.snapshot_energy(node.node_id, node.battery)

            if (
                partition_at is not None
                and not partition_injected
                and elapsed >= partition_at
                and cluster.message_bus._partitions == []
            ):
                ids = list(cluster.nodes.keys())
                mid = len(ids) // 2
                cluster.inject_partition(ids[:mid], ids[mid:])
                partition_injected = True

            if (
                heal_at is not None
                and not partition_healed
                and elapsed >= heal_at
                and cluster.message_bus._partitions
            ):
                cluster.heal_partition()
                partition_healed = True

            for node in cluster.nodes.values():
                if getattr(node, "state", None) == NodeState.LEADER:
                    collector.record_leader_change(node.node_id)

            if elapsed >= duration:
                break

    effects_task = asyncio.create_task(side_effects())
    try:
        await cluster.run(duration)
    finally:
        effects_task.cancel()
        try:
            await effects_task
        except asyncio.CancelledError:
            pass


# --------------------------------------------------------------------- CLI
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ECHO vs Raft consensus simulation",
    )
    p.add_argument(
        "--coordinators", type=int, default=5,
        help="Number of coordinator / Raft peer nodes (default: 5)",
    )
    p.add_argument(
        "--leaves", type=int, default=10,
        help="Number of ECHO leaf nodes (default: 10)",
    )
    p.add_argument(
        "--duration", type=float, default=5.0,
        help="Simulation duration in seconds (default: 5)",
    )
    p.add_argument(
        "--battery-drain", type=float, default=0.01,
        help="Battery drain rate per second (0–1 scale, default: 0.01)",
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
    )
    collectors["raft"] = raft_collector

    # --- ECHO ---
    logger.info("=== Running ECHO protocol ===")
    echo_cluster = build_echo_cluster(
        coordinator_count=args.coordinators,
        leaf_count=args.leaves,
    )
    echo_collector = MetricsCollector()
    await run_scenario(
        echo_cluster, echo_collector,
        duration=args.duration,
        battery_drain=args.battery_drain,
        partition_at=args.partition_at,
        heal_at=args.heal_at,
    )
    collectors["echo"] = echo_collector

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
