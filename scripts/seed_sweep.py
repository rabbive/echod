"""Seed-sweep experiment for §V ("Planned extensions") of the paper.

Repeats the matched-workload comparison of Table~\\ref{tab:metrics}
(python3 -m simulation.main --coordinators 5 --leaves 10 ...) over many
seeds for both the 5 s and 30 s windows used in the Evaluation section,
and reports mean +/- standard deviation per protocol per metric instead
of a single representative run.

Usage:
    python3 -m scripts.seed_sweep
    python3 -m scripts.seed_sweep --seeds 10 --durations 5,30
    python3 -m scripts.seed_sweep --seed-list 1,2,3,4,5,6,7,8,9,10

Output:
    results/seed_sweep/<duration>s/raw_metrics.csv   -- one row per (seed, protocol)
    results/seed_sweep/<duration>s/summary.csv       -- mean/std per protocol/metric
    results/seed_sweep/summary.json                  -- both windows, machine-readable
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import random
import statistics
import time
from dataclasses import asdict

from simulation.metrics.collector import MetricsCollector
from simulation.protocols.echo import build_echo_cluster
from simulation.protocols.echod import build_echod_cluster
from simulation.protocols.raft import build_raft_cluster
from simulation.main import run_scenario
from simulation.workload import generate_workload

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

METRIC_FIELDS = [
    "total_messages",
    "consensus_rounds",
    "avg_consensus_latency_ms",
    "avg_messages_per_round",
    "leader_changes",
    "availability_pct",
]

PROTOCOLS = ("raft", "echo", "echod")


async def run_one(
    protocol: str,
    seed: int,
    duration: float,
    coordinators: int,
    leaves: int,
    battery_drain: float,
    burst_interval: float,
) -> dict:
    """Run a single protocol once under a given seed/duration and return its summary."""
    random.seed(seed)
    workload = generate_workload(
        duration=duration,
        leaf_count=leaves,
        burst_interval=burst_interval,
        seed=seed,
    )

    if protocol == "raft":
        cluster = build_raft_cluster(node_count=coordinators)
    elif protocol == "echo":
        cluster = build_echo_cluster(
            coordinator_count=coordinators, leaf_count=leaves, auto_report=False,
        )
    elif protocol == "echod":
        cluster = build_echod_cluster(
            coordinator_count=coordinators, leaf_count=leaves, auto_report=False,
        )
    else:
        raise ValueError(f"unknown protocol: {protocol}")

    collector = MetricsCollector()
    await run_scenario(
        cluster, collector,
        duration=duration,
        battery_drain=battery_drain,
        partition_at=None,
        heal_at=None,
        workload=workload,
    )
    row = collector.summary()
    row.pop("energy_per_node", None)
    row.pop("messages_by_type", None)
    row["protocol"] = protocol
    row["seed"] = seed
    return row


def mean_std(values: list[float]) -> tuple[float, float]:
    m = statistics.mean(values)
    s = statistics.stdev(values) if len(values) > 1 else 0.0
    return m, s


async def sweep_duration(
    duration: float,
    seeds: list[int],
    coordinators: int,
    leaves: int,
    battery_drain: float,
    burst_interval: float,
    output_dir: str,
) -> dict:
    out_dir = os.path.join(output_dir, f"{duration:g}s")
    os.makedirs(out_dir, exist_ok=True)

    raw_rows: list[dict] = []
    t0 = time.monotonic()
    for seed in seeds:
        for protocol in PROTOCOLS:
            row = await run_one(
                protocol, seed, duration,
                coordinators, leaves, battery_drain, burst_interval,
            )
            raw_rows.append(row)
    elapsed = time.monotonic() - t0
    logger.info("duration=%gs: %d runs in %.1fs", duration, len(raw_rows), elapsed)

    # Raw CSV: one row per (seed, protocol)
    raw_path = os.path.join(out_dir, "raw_metrics.csv")
    with open(raw_path, "w", newline="") as f:
        fieldnames = ["seed", "protocol"] + METRIC_FIELDS
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in raw_rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    # Aggregate mean +/- std per protocol per metric
    summary: dict[str, dict[str, tuple[float, float]]] = {}
    for protocol in PROTOCOLS:
        proto_rows = [r for r in raw_rows if r["protocol"] == protocol]
        summary[protocol] = {}
        for metric in METRIC_FIELDS:
            values = [float(r[metric]) for r in proto_rows]
            summary[protocol][metric] = mean_std(values)

    summary_path = os.path.join(out_dir, "summary.csv")
    with open(summary_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["protocol"] + METRIC_FIELDS)
        for protocol in PROTOCOLS:
            row = [protocol]
            for metric in METRIC_FIELDS:
                m, s = summary[protocol][metric]
                row.append(f"{m:.3f} +/- {s:.3f}")
            writer.writerow(row)

    return {
        "duration": duration,
        "n_seeds": len(seeds),
        "seeds": seeds,
        "raw_csv": raw_path,
        "summary_csv": summary_path,
        "summary": {
            protocol: {
                metric: {"mean": m, "std": s}
                for metric, (m, s) in summary[protocol].items()
            }
            for protocol in PROTOCOLS
        },
    }


def print_table(duration: float, result: dict) -> None:
    print(f"\n{'=' * 72}")
    print(f"  Seed sweep: {result['n_seeds']} seeds, {duration:g}s window")
    print(f"{'=' * 72}")
    header = f"{'protocol':<8}" + "".join(f"{m:>22}" for m in METRIC_FIELDS)
    print(header)
    for protocol in PROTOCOLS:
        cells = []
        for metric in METRIC_FIELDS:
            v = result["summary"][protocol][metric]
            cells.append(f"{v['mean']:.2f}+/-{v['std']:.2f}")
        print(f"{protocol:<8}" + "".join(f"{c:>22}" for c in cells))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--seeds", type=int, default=10,
        help="Number of seeds to sweep, using seeds 1..N (default: 10)",
    )
    p.add_argument(
        "--seed-list", type=str, default=None,
        help="Comma-separated explicit seed list, overrides --seeds",
    )
    p.add_argument(
        "--durations", type=str, default="5,30",
        help="Comma-separated durations in seconds (default: 5,30, "
             "matching Table~tab:metrics and the longer-run comparison)",
    )
    p.add_argument("--coordinators", type=int, default=5)
    p.add_argument("--leaves", type=int, default=10)
    p.add_argument("--battery-drain", type=float, default=0.01)
    p.add_argument("--burst-interval", type=float, default=1.0)
    p.add_argument(
        "--output-dir", type=str, default="results/seed_sweep",
        help="Directory for the sweep output (default: results/seed_sweep)",
    )
    return p.parse_args(argv)


async def main(args: argparse.Namespace) -> None:
    if args.seed_list:
        seeds = [int(s) for s in args.seed_list.split(",")]
    else:
        seeds = list(range(1, args.seeds + 1))
    durations = [float(d) for d in args.durations.split(",")]

    os.makedirs(args.output_dir, exist_ok=True)
    all_results = {}
    for duration in durations:
        result = await sweep_duration(
            duration, seeds,
            args.coordinators, args.leaves,
            args.battery_drain, args.burst_interval,
            args.output_dir,
        )
        all_results[f"{duration:g}s"] = result
        print_table(duration, result)

    combined_path = os.path.join(args.output_dir, "summary.json")
    with open(combined_path, "w") as f:
        json.dump({"seeds": seeds, "windows": all_results}, f, indent=2)
    print(f"\nCombined summary: {combined_path}")


def cli() -> None:
    args = parse_args()
    asyncio.run(main(args))


if __name__ == "__main__":
    cli()
