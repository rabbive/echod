"""Mock leaf node — publishes fake sensor data over MQTT.

Used for hardware-free demos: simulates one or more ESP32 leaf nodes
sending temperature / humidity readings to the ECHO coordinator cluster.

Usage::

    python -m rpi.mock_leaf --leaves 5
    python -m rpi.mock_leaf --leaves 3 --interval 0.5 --broker 192.168.1.50
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import time

import paho.mqtt.client as mqtt

from rpi.config import BROKER_HOST, BROKER_PORT, CLUSTER_ID, SAMPLE_INTERVAL

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
    ) -> None:
        self.leaf_id = leaf_id
        self._client = client
        self._cluster_id = cluster_id
        self._coordinators = coordinators
        self._sensor_type = sensor_type
        self._value = 20.0 + random.uniform(-2.0, 2.0)
        self._coordinator_id: str | None = None
        self._registered = False

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

    def send_reading(self) -> None:
        """Publish a simulated sensor reading."""
        if not self._registered or self._coordinator_id is None:
            return

        self._value += random.uniform(-0.5, 0.5)
        topic = f"echo/{self._cluster_id}/rpc/{self._coordinator_id}"
        payload = {
            "sender_id": self.leaf_id,
            "recipient_id": self._coordinator_id,
            "msg_type": "sensor_data",
            "payload": {
                "leaf_id": self.leaf_id,
                "sensor_type": self._sensor_type,
                "value": round(self._value, 2),
            },
            "timestamp": time.time(),
        }
        self._client.publish(topic, json.dumps(payload))


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
        help="Seconds between sensor readings (default: %.3f)" % SAMPLE_INTERVAL,
    )
    return p.parse_args(argv)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    args = parse_args()
    coordinators = [c.strip() for c in args.coordinators.split(",") if c.strip()]

    client = mqtt.Client(client_id="echo-mock-leaves", **_CLIENT_KWARGS)
    client.connect(args.broker, args.port)
    client.loop_start()
    time.sleep(0.5)  # let the connection establish

    leaves = [
        MockLeaf(
            leaf_id=f"leaf-{i}",
            client=client,
            cluster_id=args.cluster,
            coordinators=coordinators,
        )
        for i in range(args.leaves)
    ]

    for leaf in leaves:
        leaf.register()

    logger.info("Streaming sensor data from %d mock leaves (Ctrl-C to stop)", len(leaves))
    try:
        while True:
            for leaf in leaves:
                leaf.send_reading()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()
        logger.info("Mock leaves stopped")


if __name__ == "__main__":
    main()
