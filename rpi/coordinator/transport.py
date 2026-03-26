"""MQTT transport layer for ECHO protocol nodes.

Wraps paho-mqtt to provide send / broadcast / subscribe primitives with
JSON serialisation.  Runs the paho network loop in a background thread
and bridges incoming messages into the asyncio event loop via
``call_soon_threadsafe``.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Callable

import paho.mqtt.client as mqtt

from rpi.config import BROKER_HOST, BROKER_PORT, CLUSTER_ID

# paho-mqtt 2.x requires an explicit callback API version.
_CALLBACK_API = getattr(mqtt, "CallbackAPIVersion", None)
_CLIENT_KWARGS: dict = (
    {"callback_api_version": _CALLBACK_API.VERSION2}
    if _CALLBACK_API is not None
    else {}
)

logger = logging.getLogger(__name__)

MessageHandler = Callable[[str, str, dict[str, Any]], None]


class MQTTTransport:
    """Async-friendly MQTT transport for ECHO protocol messages.

    Parameters
    ----------
    node_id:
        Unique identifier for this node (e.g. ``coord-0``).
    cluster_id:
        Logical cluster name — used as the MQTT topic prefix.
    broker_host / broker_port:
        MQTT broker address.
    """

    def __init__(
        self,
        node_id: str,
        cluster_id: str = CLUSTER_ID,
        broker_host: str = BROKER_HOST,
        broker_port: int = BROKER_PORT,
    ) -> None:
        self.node_id = node_id
        self.cluster_id = cluster_id
        self._broker_host = broker_host
        self._broker_port = broker_port

        uid = uuid.uuid4().hex[:6]
        self._client = mqtt.Client(
            client_id=f"echo-{node_id}-{uid}", **_CLIENT_KWARGS,
        )
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect

        self._handler: MessageHandler | None = None
        self._connected = False

        # Topic layout
        self._rpc_topic = f"echo/{cluster_id}/rpc/{node_id}"
        self._broadcast_topic = f"echo/{cluster_id}/broadcast"
        self._status_topic = f"echo/{cluster_id}/status/{node_id}"

    # ---------------------------------------------------------------- connect

    def connect(self) -> None:
        """Connect to the MQTT broker and start the background network loop."""
        self._client.connect(self._broker_host, self._broker_port)
        self._client.loop_start()

    def disconnect(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()

    def _on_connect(self, _client: Any, _userdata: Any, *args: Any) -> None:
        # paho-mqtt v2 callback signature can include:
        #   (client, userdata, flags, reason_code, properties)
        # Depending on MQTT version, the reason code may be an int or an
        # object with a `value` attribute. We search the args to pick the
        # correct one (not the trailing properties object).
        reason_code: Any = 0
        for a in args:
            if isinstance(a, int):
                reason_code = a
                break
            if hasattr(a, "value"):
                reason_code = a
                break

        rc_val = getattr(reason_code, "value", reason_code)
        if rc_val == 0:
            self._connected = True
            self._client.subscribe(self._rpc_topic)
            self._client.subscribe(self._broadcast_topic)
            logger.info("%s connected to MQTT broker at %s:%s",
                        self.node_id, self._broker_host, self._broker_port)
        else:
            logger.error(
                "%s MQTT connect failed (reason_code=%s)", self.node_id, reason_code
            )

    def _on_disconnect(self, _client: Any, _userdata: Any, *args: Any) -> None:
        self._connected = False
        reason_code: Any = 0
        for a in args:
            if isinstance(a, int):
                reason_code = a
                break
            if hasattr(a, "value"):
                reason_code = a
                break

        rc_val = getattr(reason_code, "value", reason_code)
        if rc_val != 0:
            logger.warning(
                "%s unexpected MQTT disconnect (reason_code=%s)",
                self.node_id,
                reason_code,
            )

    # ------------------------------------------------------------- messaging

    def set_handler(self, handler: MessageHandler) -> None:
        """Register the callback invoked for every inbound message.

        Signature: ``handler(sender_id, msg_type, payload_dict)``
        """
        self._handler = handler

    def _on_message(self, _client: Any, _userdata: Any, msg: mqtt.MQTTMessage) -> None:
        try:
            data: dict = json.loads(msg.payload.decode())
            sender = data.get("sender_id", "")
            if sender == self.node_id:
                return  # ignore own broadcasts
            msg_type = data.get("msg_type", "")
            payload = data.get("payload", {})
            if self._handler is not None:
                self._handler(sender, msg_type, payload)
        except Exception:
            logger.exception("Failed to decode MQTT message on %s", msg.topic)

    # -------------------------------------------------------------- sending

    def send(self, recipient_id: str, msg_type: str, payload: dict) -> None:
        """Send a directed message to a specific node."""
        topic = f"echo/{self.cluster_id}/rpc/{recipient_id}"
        self._publish(topic, recipient_id, msg_type, payload)

    def broadcast(self, msg_type: str, payload: dict) -> None:
        """Publish a message to the cluster broadcast topic."""
        self._publish(self._broadcast_topic, "*", msg_type, payload)

    def publish_status(self, status: dict) -> None:
        """Publish a retained status message for the dashboard."""
        blob = json.dumps({"node_id": self.node_id, "timestamp": time.time(), **status})
        self._client.publish(self._status_topic, blob, retain=True)

    def _publish(self, topic: str, recipient: str, msg_type: str, payload: dict) -> None:
        envelope = {
            "sender_id": self.node_id,
            "recipient_id": recipient,
            "msg_type": msg_type,
            "payload": payload,
            "timestamp": time.time(),
        }
        self._client.publish(topic, json.dumps(envelope))

    # ------------------------------------------------------------- helpers

    @property
    def is_connected(self) -> bool:
        return self._connected
