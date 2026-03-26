"""Real-time dashboard for ECHO cluster monitoring.

Subscribes to MQTT status topics published by each coordinator and
pushes live updates to the browser via Socket.IO.

Usage::

    python -m rpi.dashboard.app          # default broker=localhost
    python -m rpi.dashboard.app --broker 192.168.1.50
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from threading import Lock

import paho.mqtt.client as mqtt
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO

from rpi.config import (
    BROKER_HOST,
    BROKER_PORT,
    CLUSTER_ID,
    DASHBOARD_HOST,
    DASHBOARD_PORT,
)

logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["SECRET_KEY"] = "echo-dashboard"
socketio = SocketIO(app, cors_allowed_origins="*")

# Shared state guarded by a lock (MQTT thread → Flask thread)
_nodes: dict[str, dict] = {}
_message_log: list[dict] = []
_lock = Lock()

MAX_MESSAGE_LOG = 500


# ---------------------------------------------------------------- MQTT setup

_CALLBACK_API = getattr(mqtt, "CallbackAPIVersion", None)
_CLIENT_KWARGS: dict = (
    {"callback_api_version": _CALLBACK_API.VERSION2}
    if _CALLBACK_API is not None
    else {}
)


def _setup_mqtt(broker_host: str, broker_port: int, cluster_id: str) -> mqtt.Client:
    client = mqtt.Client(client_id="echo-dashboard", **_CLIENT_KWARGS)

    def on_connect(_client: mqtt.Client, _ud: object, *args: object) -> None:
        # paho-mqtt v2 callback signature may include:
        #   (client, userdata, flags, reason_code, properties)
        # Choose the reason_code from args (not the trailing properties).
        reason_code: object = 0
        for a in args:
            if isinstance(a, int):
                reason_code = a
                break
            if hasattr(a, "value"):
                reason_code = a
                break

        rc_val = getattr(reason_code, "value", reason_code)
        if rc_val == 0:
            _client.subscribe(f"echo/{cluster_id}/status/+")
            _client.subscribe(f"echo/{cluster_id}/broadcast")
            logger.info("Dashboard connected to MQTT broker")
        else:
            logger.error(
                "Dashboard MQTT connect failed (reason_code=%s)", reason_code
            )

    def on_message(_client: mqtt.Client, _ud: object, msg: mqtt.MQTTMessage) -> None:
        try:
            data = json.loads(msg.payload.decode())
            if "/status/" in msg.topic:
                node_id = data.get("node_id", "unknown")
                with _lock:
                    _nodes[node_id] = data
                socketio.emit("node_update", data)
            elif "/broadcast" in msg.topic:
                with _lock:
                    _message_log.append(data)
                    if len(_message_log) > MAX_MESSAGE_LOG:
                        del _message_log[: len(_message_log) - MAX_MESSAGE_LOG]
                socketio.emit("broadcast_msg", data)
        except Exception:
            logger.exception("Failed to process MQTT message")

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker_host, broker_port)
    client.loop_start()
    return client


# -------------------------------------------------------------- Flask routes

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/nodes")
def api_nodes():
    with _lock:
        return jsonify(list(_nodes.values()))


@app.route("/api/messages")
def api_messages():
    with _lock:
        return jsonify(_message_log[-100:])


# -------------------------------------------------------------------- main

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ECHO cluster dashboard")
    p.add_argument("--broker", default=BROKER_HOST, help="MQTT broker host")
    p.add_argument("--port", type=int, default=BROKER_PORT, help="MQTT broker port")
    p.add_argument("--cluster", default=CLUSTER_ID, help="Cluster ID")
    p.add_argument("--host", default=DASHBOARD_HOST, help="Dashboard bind host")
    p.add_argument("--dash-port", type=int, default=DASHBOARD_PORT, help="Dashboard port")
    return p.parse_args(argv)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    args = parse_args()
    _setup_mqtt(args.broker, args.port, args.cluster)
    logger.info("Dashboard starting on http://%s:%d", args.host, args.dash_port)
    socketio.run(
        app,
        host=args.host,
        port=args.dash_port,
        allow_unsafe_werkzeug=True,
    )


if __name__ == "__main__":
    main()
