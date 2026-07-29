"""Mock leaf node — publishes fake sensor data over MQTT.

Used for hardware-free demos and for the experiment harness: simulates
one or more ESP32 leaf nodes sending temperature / humidity readings to
the coordinator cluster.

Two workload modes:

- ``stream`` (default) — unseeded random walk, one reading per leaf per
  interval.  Demo behaviour; not for measurements.
- ``bursts`` — seeded workload mirroring ``simulation/workload.py``:
  once per burst interval every leaf emits one reading at the same
  instant; values follow a per-leaf random walk with ~30 % large jumps
  (delta-threshold breaches) and ~70 % small drifts.  Each leaf uses its
  own RNG stream (``seed + leaf_index``), so the schedule is identical
  across protocols and runs — differences come from the protocols, not
  the traffic.

Usage::

    python -m rpi.mock_leaf --leaves 5
    python -m rpi.mock_leaf --workload bursts --seed 42 --leaves 10 \
        --burst-interval 1.0 --duration 30 --edge-filter --stats
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import threading
import time

import paho.mqtt.client as mqtt

from rpi.config import (
    BROKER_HOST,
    BROKER_PORT,
    CLUSTER_ID,
    DELTA_THRESHOLD,
    SAMPLE_INTERVAL,
)

_CALLBACK_API = getattr(mqtt, "CallbackAPIVersion", None)
_CLIENT_KWARGS: dict = (
    {"callback_api_version": _CALLBACK_API.VERSION2}
    if _CALLBACK_API is not None
    else {}
)

logger = logging.getLogger(__name__)


class MockLeaf:
    """Simulates a single leaf node that registers and streams sensor data."""

    def __init__(
        self,
        leaf_id: str,
        client: mqtt.Client,
        cluster_id: str,
        coordinators: list[str],
        sensor_type: str = "temperature",
        edge_filter: bool = False,
        workload: str = "stream",
        seed: int = 42,
        leaf_index: int = 0,
    ) -> None:
        self.leaf_id = leaf_id
        self._client = client
        self._cluster_id = cluster_id
        self._coordinators = coordinators
        self._sensor_type = sensor_type
        self._edge_filter = edge_filter
        self._workload = workload
        # Per-leaf RNG stream — identical across protocols and runs.
        self._rng = random.Random(seed * 1000 + leaf_index)
        if workload == "bursts":
            self._value = 20.0 + self._rng.uniform(-2.0, 2.0)
        else:
            self._value = 20.0 + random.uniform(-2.0, 2.0)
        self._last_transmitted: float | None = None
        self._coordinator_id: str | None = None
        self._registered = False
        # Harness counters (reported via --stats)
        self.sent_count: int = 0
        self.suppressed_count: int = 0
        self.recv_by_type: dict[str, int] = {}

    def register(self) -> None:
        """Send a registration request to a random coordinator."""
        target = random.choice(self._coordinators)
        topic = f"echo/{self._cluster_id}/rpc/{target}"
        payload = {
            "sender_id": self.leaf_id,
            "recipient_id": target,
            "msg_type": "leaf_register",
            "payload": {
                "leaf_id": self.leaf_id,
                "capabilities": {"sensor": self._sensor_type},
            },
            "timestamp": time.time(),
        }
        self._client.publish(topic, json.dumps(payload))
        logger.info("%s sent registration request to %s", self.leaf_id, target)
        self._coordinator_id = target
        self._registered = True

    def next_burst_value(self) -> float:
        """Advance the seeded random walk (mirrors simulation/workload.py).

        ~30 % of steps are large jumps (10–25 % of ~20 — breaching
        DELTA_THRESHOLD); the rest are small sub-threshold drifts.
        """
        if self._rng.random() < 0.3:
            step = self._rng.choice((-1.0, 1.0)) * self._rng.uniform(2.0, 5.0)
        else:
            step = self._rng.uniform(-0.4, 0.4)
        self._value += step
        return self._value

    def send_reading(self, value: float | None = None) -> None:
        """Publish a sensor reading (stream mode draws its own step).

        With ``edge_filter`` enabled (echoD optimization 1), sub-threshold
        readings are suppressed at the leaf and never transmitted — the
        radio cost of a filtered reading is zero.
        """
        if not self._registered or self._coordinator_id is None:
            return

        if value is None:
            self._value += random.uniform(-0.5, 0.5)
        else:
            self._value = value

        if self._edge_filter and self._last_transmitted is not None:
            delta = abs(self._value - self._last_transmitted) / max(
                abs(self._last_transmitted), 1e-9,
            )
            if delta < DELTA_THRESHOLD:
                self.suppressed_count += 1
                return

        self._last_transmitted = self._value
        topic = f"echo/{self._cluster_id}/rpc/{self._coordinator_id}"
        payload = {
            "sender_id": self.leaf_id,
            "recipient_id": self._coordinator_id,
            "msg_type": "sensor_data",
            "payload": {
                "leaf_id": self.leaf_id,
                "sensor_type": self._sensor_type,
                "value": round(self._value, 2),
                "sent_at": time.time(),
            },
            "timestamp": time.time(),
        }
        self._client.publish(topic, json.dumps(payload))
        self.sent_count += 1

    def note_inbound(self, msg_type: str) -> None:
        self.recv_by_type[msg_type] = self.recv_by_type.get(msg_type, 0) + 1

    def stats_payload(self) -> dict:
        return {
            "leaf_id": self.leaf_id,
            "sent": self.sent_count,
            "suppressed": self.suppressed_count,
            "recv_by_type": dict(self.recv_by_type),
            "timestamp": time.time(),
        }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mock ESP32 leaf nodes")
    p.add_argument("--leaves", type=int, default=5, help="Number of mock leaves")
    p.add_argument(
        "--coordinators", default="coord-0,coord-1,coord-2,coord-3,coord-4",
        help="Comma-separated coordinator IDs to register with",
    )
    p.add_argument("--broker", default=BROKER_HOST)
    p.add_argument("--port", type=int, default=BROKER_PORT)
    p.add_argument("--cluster", default=CLUSTER_ID)
    p.add_argument(
        "--interval", type=float, default=SAMPLE_INTERVAL,
        help="Stream mode: seconds between readings (default: %.3f)"
        % SAMPLE_INTERVAL,
    )
    p.add_argument(
        "--edge-filter", action="store_true",
        help="Suppress sub-threshold readings at the leaf (echoD mode)",
    )
    p.add_argument(
        "--workload", choices=["stream", "bursts"], default="stream",
        help="stream = unseeded demo walk; bursts = seeded harness schedule",
    )
    p.add_argument("--seed", type=int, default=42, help="Bursts workload seed")
    p.add_argument(
        "--burst-interval", type=float, default=1.0,
        help="Bursts mode: seconds between synchronized bursts",
    )
    p.add_argument(
        "--start-delay", type=float, default=2.0,
        help="Bursts mode: wait this long before the first burst (election settle)",
    )
    p.add_argument(
        "--duration", type=float, default=0.0,
        help="Stop after N seconds (0 = run forever)",
    )
    p.add_argument(
        "--stats", action="store_true",
        help="Publish per-leaf counters to echo/<cluster>/stats/<leaf_id>",
    )
    return p.parse_args(argv)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    args = parse_args()
    coordinators = [c.strip() for c in args.coordinators.split(",") if c.strip()]
    demo_topic = f"echo/{args.cluster}/demo/leaves"
    broadcast_topic = f"echo/{args.cluster}/broadcast"
    rpc_wildcard = f"echo/{args.cluster}/rpc/+"
    pause_state: dict[str, bool] = {"paused": False}
    pause_lock = threading.Lock()

    client = mqtt.Client(client_id="echo-mock-leaves", **_CLIENT_KWARGS)

    leaves = [
        MockLeaf(
            leaf_id=f"leaf-{i}",
            client=client,
            cluster_id=args.cluster,
            coordinators=coordinators,
            edge_filter=args.edge_filter,
            workload=args.workload,
            seed=args.seed,
            leaf_index=i,
        )
        for i in range(args.leaves)
    ]
    by_id = {leaf.leaf_id: leaf for leaf in leaves}

    def on_message(_client: mqtt.Client, _ud: object, msg: mqtt.MQTTMessage) -> None:
        if msg.topic == demo_topic:
            try:
                data = json.loads(msg.payload.decode())
                if "paused" in data:
                    with pause_lock:
                        pause_state["paused"] = bool(data["paused"])
                    logger.info(
                        "Mock leaves streaming paused=%s", pause_state["paused"],
                    )
            except Exception:
                logger.exception("Failed to parse demo leaves control message")
            return
        # Count inbound protocol traffic for the harness.  Broadcasts are
        # heard by every leaf (shared radio channel); directed messages are
        # attributed to their recipient via the envelope.
        try:
            data = json.loads(msg.payload.decode())
            msg_type = data.get("msg_type", "")
            recipient = data.get("recipient_id", "")
            if recipient == "*":
                for leaf in leaves:
                    leaf.note_inbound(msg_type)
            elif recipient in by_id:
                by_id[recipient].note_inbound(msg_type)
        except Exception:
            pass

    client.on_message = on_message
    client.connect(args.broker, args.port)
    client.loop_start()
    time.sleep(0.5)  # let the connection establish
    client.subscribe(demo_topic)
    client.subscribe(broadcast_topic)
    client.subscribe(rpc_wildcard)

    for leaf in leaves:
        leaf.register()

    logger.info(
        "Streaming sensor data from %d mock leaves (workload=%s, Ctrl-C to stop)",
        len(leaves), args.workload,
    )
    started = time.monotonic()
    last_stats = 0.0
    try:
        if args.workload == "bursts":
            next_burst = started + args.start_delay
            while True:
                now = time.monotonic()
                if args.duration and now - started >= args.duration:
                    break
                if now >= next_burst:
                    with pause_lock:
                        paused = pause_state["paused"]
                    if not paused:
                        for leaf in leaves:
                            leaf.send_reading(leaf.next_burst_value())
                    next_burst += args.burst_interval
                if args.stats and now - last_stats >= 2.0:
                    last_stats = now
                    for leaf in leaves:
                        client.publish(
                            f"echo/{args.cluster}/stats/{leaf.leaf_id}",
                            json.dumps(leaf.stats_payload()),
                        )
                time.sleep(0.01)
        else:
            while True:
                now = time.monotonic()
                if args.duration and now - started >= args.duration:
                    break
                with pause_lock:
                    paused = pause_state["paused"]
                if not paused:
                    for leaf in leaves:
                        leaf.send_reading()
                if args.stats and now - last_stats >= 2.0:
                    last_stats = now
                    for leaf in leaves:
                        client.publish(
                            f"echo/{args.cluster}/stats/{leaf.leaf_id}",
                            json.dumps(leaf.stats_payload()),
                        )
                time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        # Final stats flush so the harness always has terminal counters.
        if args.stats:
            for leaf in leaves:
                client.publish(
                    f"echo/{args.cluster}/stats/{leaf.leaf_id}",
                    json.dumps(leaf.stats_payload()),
                )
            time.sleep(0.3)
        client.loop_stop()
        client.disconnect()
        logger.info("Mock leaves stopped")


if __name__ == "__main__":
    main()
