"""Experiment metrics collector (Phase 2 harness).

Subscribes to the cluster's status topics (coordinators) and stats
topics (mock leaves), samples the run at a fixed cadence, then writes
CSV/JSON artifacts in the SAME schema as the simulation reporter so
hardware results can be compared directly with published sim numbers:

- ``metrics.csv``    — protocol,total_messages,consensus_rounds,
  avg_consensus_latency_ms,avg_messages_per_round,leader_changes,
  availability_pct   (identical fieldnames to simulation/metrics/reporter.py)
- ``nodes.csv``      — per-node detail (final battery, TX/RX per type…)
- ``timeseries.csv`` — sampled per-node battery/state/term/log columns
- ``summary.json``   — run metadata + headline numbers

Accounting matches the simulation's delivery-based semantics: a message
is counted once per node that receives it (so a broadcast ping costs
N deliveries, exactly as the sim counts it).  ``partition_control`` and
``demo_control`` are harness plumbing and are excluded from totals.

Usage::

    python -m rpi.metrics_collector --protocol echod --duration 30 \
        --seed 42 --out results/hw/echod/42
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import threading
import time

import paho.mqtt.client as mqtt

from rpi.config import BROKER_HOST, BROKER_PORT, CLUSTER_ID

_CALLBACK_API = getattr(mqtt, "CallbackAPIVersion", None)
_CLIENT_KWARGS: dict = (
    {"callback_api_version": _CALLBACK_API.VERSION2}
    if _CALLBACK_API is not None
    else {}
)

logger = logging.getLogger(__name__)

# Harness plumbing message types — excluded from protocol-traffic totals.
_PLUMBING_TYPES = {"partition_control", "demo_control"}

METRICS_FIELDNAMES = [
    "protocol",
    "total_messages",
    "consensus_rounds",
    "avg_consensus_latency_ms",
    "avg_messages_per_round",
    "leader_changes",
    "availability_pct",
]


class MetricsCollector:
    """Collects coordinator status + leaf stats over one experiment run."""

    def __init__(self, cluster_id: str = CLUSTER_ID) -> None:
        self._cluster_id = cluster_id
        self._lock = threading.Lock()
        # node_id -> latest coordinator status dict
        self._coordinators: dict[str, dict] = {}
        # leaf_id -> latest leaf stats dict
        self._leaves: dict[str, dict] = {}
        # (monotonic_ts, has_leader)
        self._availability: list[tuple[float, bool]] = []
        # timeseries rows
        self._samples: list[dict] = []

    # ---------------------------------------------------------- ingestion

    def handle_message(self, topic: str, payload: bytes) -> None:
        try:
            data = json.loads(payload.decode())
        except Exception:
            return
        parts = topic.split("/")
        # echo/<cluster>/status/<node>  |  echo/<cluster>/stats/<leaf>
        if len(parts) < 4 or parts[0] != "echo" or parts[1] != self._cluster_id:
            return
        kind, node_id = parts[2], parts[3]
        with self._lock:
            if kind == "status" and isinstance(data, dict) and "state" in data:
                self._coordinators[node_id] = data
            elif kind == "stats" and isinstance(data, dict) and "sent" in data:
                self._leaves[node_id] = data

    def sample(self) -> None:
        """Record one availability + timeseries sample (call at cadence)."""
        now = time.time()
        with self._lock:
            coords = dict(self._coordinators)
            has_leader = any(
                s.get("state") in ("leader", "local_leader")
                for s in coords.values()
            )
            self._availability.append((now, has_leader))
            for node_id, s in coords.items():
                self._samples.append({
                    "time": round(now, 3),
                    "node_id": node_id,
                    "state": s.get("state", ""),
                    "term": s.get("term", ""),
                    "battery": s.get("battery", ""),
                    "log_length": s.get("log_length", ""),
                    "commit_index": s.get("commit_index", ""),
                    "partition_epoch": s.get("partition_epoch", 0),
                })

    # ---------------------------------------------------------- aggregation

    def summary(self, protocol: str) -> dict:
        """Sim-compatible headline metrics for one finished run."""
        with self._lock:
            coords = dict(self._coordinators)
            leaves = dict(self._leaves)
            availability = list(self._availability)

        def counted(by_type: dict) -> int:
            return sum(
                n for t, n in by_type.items() if t not in _PLUMBING_TYPES
            )

        # Delivery-side totals (sim semantics): every inbound protocol
        # message at coordinators + leaves.
        coord_rx = sum(
            counted(s.get("messages_recv_by_type", {})) for s in coords.values()
        )
        leaf_rx = sum(
            counted(s.get("recv_by_type", {})) for s in leaves.values()
        )
        total_messages = coord_rx + leaf_rx

        # Aggregate received-by-type across the cluster (for summary.json).
        recv_by_type: dict[str, int] = {}
        for s in coords.values():
            for t, n in s.get("messages_recv_by_type", {}).items():
                if t not in _PLUMBING_TYPES:
                    recv_by_type[t] = recv_by_type.get(t, 0) + n
        for s in leaves.values():
            for t, n in s.get("recv_by_type", {}).items():
                if t not in _PLUMBING_TYPES:
                    recv_by_type[t] = recv_by_type.get(t, 0) + n

        rounds = sum(s.get("consensus_rounds", 0) for s in coords.values())
        latency_weighted = sum(
            s.get("avg_consensus_latency_ms", 0.0) * s.get("consensus_rounds", 0)
            for s in coords.values()
        )
        avg_latency = latency_weighted / rounds if rounds else 0.0
        leader_changes = sum(s.get("leader_changes", 0) for s in coords.values())
        avail_pct = (
            100.0 * sum(1 for _, ok in availability if ok) / len(availability)
            if availability else 0.0
        )

        return {
            "protocol": protocol,
            "total_messages": total_messages,
            "consensus_rounds": rounds,
            "avg_consensus_latency_ms": round(avg_latency, 3),
            "avg_messages_per_round": (
                round(total_messages / rounds, 1) if rounds else 0.0
            ),
            "leader_changes": leader_changes,
            "availability_pct": round(avail_pct, 2),
            "messages_recv_by_type": recv_by_type,
            "coordinator_rx": coord_rx,
            "leaf_rx": leaf_rx,
        }

    # ---------------------------------------------------------- output

    def write(self, out_dir: str, protocol: str, meta: dict) -> dict:
        os.makedirs(out_dir, exist_ok=True)
        row = self.summary(protocol)

        with open(os.path.join(out_dir, "metrics.csv"), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=METRICS_FIELDNAMES)
            writer.writeheader()
            writer.writerow({k: row.get(k, "") for k in METRICS_FIELDNAMES})

        with self._lock:
            coords = dict(self._coordinators)
            leaves = dict(self._leaves)
            samples = list(self._samples)

        with open(os.path.join(out_dir, "nodes.csv"), "w", newline="") as f:
            fields = [
                "node_id", "kind", "final_state", "final_battery",
                "tx_total", "rx_total", "consensus_rounds",
                "avg_consensus_latency_ms", "leader_changes",
                "sent_by_type", "recv_by_type",
            ]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for node_id, s in sorted(coords.items()):
                writer.writerow({
                    "node_id": node_id,
                    "kind": "coordinator",
                    "final_state": s.get("state", ""),
                    "final_battery": s.get("battery", ""),
                    "tx_total": s.get("messages_sent", 0),
                    "rx_total": s.get("messages_received", 0),
                    "consensus_rounds": s.get("consensus_rounds", 0),
                    "avg_consensus_latency_ms": s.get(
                        "avg_consensus_latency_ms", 0,
                    ),
                    "leader_changes": s.get("leader_changes", 0),
                    "sent_by_type": json.dumps(
                        s.get("messages_sent_by_type", {}), sort_keys=True,
                    ),
                    "recv_by_type": json.dumps(
                        s.get("messages_recv_by_type", {}), sort_keys=True,
                    ),
                })
            for leaf_id, s in sorted(leaves.items()):
                writer.writerow({
                    "node_id": leaf_id,
                    "kind": "leaf",
                    "final_state": "",
                    "final_battery": "",
                    "tx_total": s.get("sent", 0),
                    "rx_total": sum(s.get("recv_by_type", {}).values()),
                    "consensus_rounds": "",
                    "avg_consensus_latency_ms": "",
                    "leader_changes": "",
                    "sent_by_type": json.dumps(
                        {"sensor_data": s.get("sent", 0),
                         "suppressed": s.get("suppressed", 0)},
                        sort_keys=True,
                    ),
                    "recv_by_type": json.dumps(
                        s.get("recv_by_type", {}), sort_keys=True,
                    ),
                })

        with open(os.path.join(out_dir, "timeseries.csv"), "w", newline="") as f:
            fields = [
                "time", "node_id", "state", "term", "battery",
                "log_length", "commit_index", "partition_epoch",
            ]
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(samples)

        with open(os.path.join(out_dir, "summary.json"), "w") as f:
            json.dump({**meta, **row}, f, indent=2, sort_keys=True)

        return row


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Experiment metrics collector")
    p.add_argument("--protocol", required=True)
    p.add_argument("--duration", type=float, required=True,
                   help="Seconds to collect before writing CSVs and exiting")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", required=True, help="Output directory for CSVs")
    p.add_argument("--broker", default=BROKER_HOST)
    p.add_argument("--port", type=int, default=BROKER_PORT)
    p.add_argument("--cluster", default=CLUSTER_ID)
    p.add_argument("--sample-interval", type=float, default=0.25)
    p.add_argument("--coordinators", type=int, default=5)
    p.add_argument("--leaves", type=int, default=5)
    p.add_argument("--partition-at", type=float, default=None)
    p.add_argument("--heal-at", type=float, default=None)
    return p.parse_args(argv)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    args = parse_args()
    collector = MetricsCollector(cluster_id=args.cluster)

    client = mqtt.Client(
        client_id=f"echo-collector-{int(time.time())}", **_CLIENT_KWARGS,
    )
    client.on_message = lambda _c, _u, msg: collector.handle_message(
        msg.topic, msg.payload,
    )
    client.connect(args.broker, args.port)
    client.subscribe(f"echo/{args.cluster}/status/#")
    client.subscribe(f"echo/{args.cluster}/stats/#")
    client.loop_start()

    logger.info(
        "collecting for %.1fs (protocol=%s seed=%d) -> %s",
        args.duration, args.protocol, args.seed, args.out,
    )
    started = time.monotonic()
    next_sample = started
    try:
        while time.monotonic() - started < args.duration:
            now = time.monotonic()
            if now >= next_sample:
                collector.sample()
                next_sample += args.sample_interval
            time.sleep(0.02)
    except KeyboardInterrupt:
        pass
    finally:
        time.sleep(0.5)  # let final retained statuses arrive
        client.loop_stop()
        client.disconnect()

    meta = {
        "seed": args.seed,
        "duration_s": args.duration,
        "coordinators": args.coordinators,
        "leaves": args.leaves,
        "partition_at": args.partition_at,
        "heal_at": args.heal_at,
        "transport": "mqtt",
        "collected_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    row = collector.write(args.out, args.protocol, meta)
    logger.info("wrote results to %s", args.out)
    print(json.dumps(row, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
