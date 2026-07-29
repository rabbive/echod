"""Partition injection helper for the experiment harness.

Publishes a single ``partition_control`` broadcast that every coordinator
uses to block/restore inbound traffic (the mirror image of the
simulation's message-bus partition).  ``partition_control`` messages are
exempt from blocking, so a heal always reaches both sides.

Usage::

    python -m rpi.partition_ctl partition --side coord-0,coord-1 \
        --side coord-2,coord-3,coord-4
    python -m rpi.partition_ctl heal

On real hardware you can alternatively use iptables for a true
network-level split; this hook exists so the same runner works on
localhost (single broker) and across machines.
"""

from __future__ import annotations

import argparse
import json
import logging
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Inject or heal a cluster partition")
    p.add_argument("action", choices=["partition", "heal"])
    p.add_argument(
        "--side", action="append", default=[],
        help="Comma-separated node IDs on one side (repeat per side; "
             "partition action only)",
    )
    p.add_argument("--broker", default=BROKER_HOST)
    p.add_argument("--port", type=int, default=BROKER_PORT)
    p.add_argument("--cluster", default=CLUSTER_ID)
    return p.parse_args(argv)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    args = parse_args()

    payload: dict = {"action": args.action}
    if args.action == "partition":
        groups = [
            [nid.strip() for nid in side.split(",") if nid.strip()]
            for side in args.side
        ]
        groups = [g for g in groups if g]
        if len(groups) < 2:
            raise SystemExit("partition requires at least two --side groups")
        payload["groups"] = groups

    client = mqtt.Client(
        client_id=f"echo-partition-ctl-{int(time.time())}", **_CLIENT_KWARGS,
    )
    client.connect(args.broker, args.port)
    client.loop_start()
    time.sleep(0.3)
    topic = f"echo/{args.cluster}/broadcast"
    envelope = {
        "sender_id": "partition-ctl",
        "recipient_id": "*",
        "msg_type": "partition_control",
        "payload": payload,
        "timestamp": time.time(),
    }
    client.publish(topic, json.dumps(envelope))
    logger.info("published %s to %s: %s", args.action, topic, payload)
    time.sleep(0.3)
    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    main()
