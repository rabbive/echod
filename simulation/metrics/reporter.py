"""Metrics reporter — CSV export and matplotlib chart generation."""

from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Any

from simulation.metrics.collector import MetricsCollector


def export_csv(
    collectors: dict[str, MetricsCollector],
    output_dir: str = "results",
) -> str:
    """Write a comparison CSV with one row per protocol.

    Returns the path to the written file.
    """
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "metrics.csv")

    fieldnames = [
        "protocol",
        "total_messages",
        "consensus_rounds",
        "avg_consensus_latency_ms",
        "avg_messages_per_round",
        "leader_changes",
        "availability_pct",
    ]

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for name, col in collectors.items():
            row = col.summary()
            row["protocol"] = name
            row.pop("energy_per_node", None)
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    return path


def export_energy_csv(
    collectors: dict[str, MetricsCollector],
    output_dir: str = "results",
) -> str:
    """Write per-node energy consumption to a CSV."""
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "energy.csv")

    fieldnames = ["protocol", "node_id", "energy_consumed"]

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for name, col in collectors.items():
            for nid, energy in col.energy_per_node.items():
                writer.writerow({
                    "protocol": name,
                    "node_id": nid,
                    "energy_consumed": round(energy, 6),
                })

    return path


def generate_charts(
    collectors: dict[str, MetricsCollector],
    output_dir: str = "results",
) -> list[str]:
    """Generate comparison bar charts using matplotlib.

    Returns a list of saved figure paths.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return []

    os.makedirs(output_dir, exist_ok=True)
    paths: list[str] = []

    protocols = list(collectors.keys())

    # --- Chart 1: Latency & Messages ---
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))

    latencies = [c.avg_consensus_latency_ms for c in collectors.values()]
    axes[0].bar(protocols, latencies)
    axes[0].set_ylabel("ms")
    axes[0].set_title("Avg Consensus Latency")

    msgs = [c.avg_messages_per_round for c in collectors.values()]
    axes[1].bar(protocols, msgs)
    axes[1].set_ylabel("messages")
    axes[1].set_title("Avg Messages per Round")

    fig.tight_layout()
    p = os.path.join(output_dir, "latency_messages.png")
    fig.savefig(p, dpi=150)
    plt.close(fig)
    paths.append(p)

    # --- Chart 2: Energy per node ---
    fig, ax = plt.subplots(figsize=(10, 4))
    for name, col in collectors.items():
        epn = col.energy_per_node
        if epn:
            nodes = sorted(epn.keys())
            values = [epn[n] for n in nodes]
            ax.bar(
                [f"{name}\n{n}" for n in nodes],
                values,
                label=name,
            )
    ax.set_ylabel("Energy consumed (0-1)")
    ax.set_title("Energy Consumption per Node")
    ax.legend()
    fig.tight_layout()
    p = os.path.join(output_dir, "energy.png")
    fig.savefig(p, dpi=150)
    plt.close(fig)
    paths.append(p)

    # --- Chart 3: Availability ---
    fig, ax = plt.subplots(figsize=(6, 4))
    avail = [c.availability_pct for c in collectors.values()]
    ax.bar(protocols, avail)
    ax.set_ylabel("%")
    ax.set_ylim(0, 105)
    ax.set_title("Cluster Availability")
    fig.tight_layout()
    p = os.path.join(output_dir, "availability.png")
    fig.savefig(p, dpi=150)
    plt.close(fig)
    paths.append(p)

    return paths
