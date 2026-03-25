#!/usr/bin/env bash
# Deploy the Phase 2 RPi coordinator code to one or more Raspberry Pis.
#
# Usage:
#   bash scripts/deploy_rpi.sh pi@192.168.1.10 pi@192.168.1.11
#
# Prerequisites on each Pi:
#   - Python 3.11+
#   - pip install -r rpi/requirements.txt
#   - A running MQTT broker (mosquitto) reachable from all nodes

set -euo pipefail
cd "$(dirname "$0")/.."

if [ $# -eq 0 ]; then
    echo "Usage: $0 <user@host> [user@host ...]"
    echo "  Copies rpi/ to each host and prints how to start a node."
    exit 1
fi

REMOTE_DIR="/home/pi/echo-protocol"

for HOST in "$@"; do
    echo "--- Deploying to $HOST ---"
    ssh "$HOST" "mkdir -p $REMOTE_DIR"
    rsync -avz --exclude '__pycache__' rpi/ "$HOST:$REMOTE_DIR/rpi/"
    echo "  Deployed.  To start a coordinator on $HOST:"
    echo "    cd $REMOTE_DIR"
    echo "    python -m rpi.coordinator.echo_node \\"
    echo "        --node-id coord-X --peers coord-0,coord-1,... --broker <BROKER_IP>"
    echo ""
done

echo "To start the dashboard on any machine:"
echo "  python -m rpi.dashboard.app --broker <BROKER_IP>"
