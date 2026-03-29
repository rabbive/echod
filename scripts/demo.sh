#!/usr/bin/env bash
# One-command hardware-free demo of the ECHO protocol (Phase 2).
#
# Starts:
#   1. A local Mosquitto MQTT broker (if installed)
#   2. Five ECHO coordinator nodes (mock battery)
#   3. Five mock leaf nodes sending sensor data
#   4. The real-time Flask dashboard
#
# Usage:
#   bash scripts/demo.sh          # start everything
#   bash scripts/demo.sh stop     # kill background processes
#
# Optional environment:
#   ECHO_DEMO_LOW_BATTERY=1  — start coordinators with mock battery ~12–28%
#                              so observer / energy gating shows sooner (~30s).
#   DEMO_BATTERY_BASE=N      — if set (and ECHO_DEMO_LOW_BATTERY unset), each node
#                              uses battery in [N, N+19] capped at 100 (default: random 60–99).
#   ECHO_DEMO=0              — set on coordinator processes to ignore demo_control MQTT
#                              (export before starting nodes manually; demo.sh does not set it).
#
# macOS: port 5000 is often used by AirPlay; this script scans 5000–5010 for the dashboard.
#
# Requirements:
#   brew install mosquitto         # macOS
#   pip install -r rpi/requirements.txt

set -euo pipefail
cd "$(dirname "$0")/.."

PIDS_FILE="/tmp/echo-demo-pids"
COORD_COUNT=5
LEAF_COUNT=5
DASH_PORT=5000

# ---------------------------------------------------------------- stop
if [ "${1:-}" = "stop" ]; then
    echo "Stopping ECHO demo …"
    if [ -f "$PIDS_FILE" ]; then
        while read -r pid; do
            kill "$pid" 2>/dev/null || true
        done < "$PIDS_FILE"
        rm "$PIDS_FILE"
    fi
    # also kill any mosquitto we started
    pkill -f "mosquitto -p 1883" 2>/dev/null || true
    echo "Done."
    exit 0
fi

# ----------------------------------------------------------- preflight
command -v python3 >/dev/null 2>&1 || { echo "python3 required"; exit 1; }
PYTHON=python3

echo "============================================"
echo "  ECHO Protocol — Hardware-Free Demo"
echo "============================================"
echo ""

# -------------------------------------------------------- mosquitto
if command -v mosquitto >/dev/null 2>&1; then
    echo "[1/4] Starting Mosquitto MQTT broker …"
    mosquitto -p 1883 -d 2>/dev/null || true
    sleep 1
else
    echo "[1/4] Mosquitto not found — assuming broker already running on localhost:1883"
fi

# Build the peers list (coord-0 … coord-N)
PEERS=""
for i in $(seq 0 $((COORD_COUNT - 1))); do
    [ -n "$PEERS" ] && PEERS="$PEERS,"
    PEERS="${PEERS}coord-${i}"
done

# ------------------------------------------------------ coordinators
echo "[2/4] Starting $COORD_COUNT coordinator nodes (mock battery) …"
> "$PIDS_FILE"
for i in $(seq 0 $((COORD_COUNT - 1))); do
    if [ "${ECHO_DEMO_LOW_BATTERY:-0}" = "1" ]; then
        BATT="$(( 12 + RANDOM % 17 ))"
    elif [ -n "${DEMO_BATTERY_BASE:-}" ]; then
        BASE="$DEMO_BATTERY_BASE"
        BATT="$(( BASE + RANDOM % 20 ))"
        [ "$BATT" -gt 100 ] && BATT=100
    else
        BATT="$(( 60 + RANDOM % 40 ))"
    fi
    $PYTHON -m rpi.coordinator.echo_node \
        --node-id "coord-${i}" \
        --peers "$PEERS" \
        --mock \
        --battery "$BATT" \
        > "/tmp/echo-coord-${i}.log" 2>&1 &
    echo $! >> "$PIDS_FILE"
done
sleep 2

# -------------------------------------------------------- mock leaves
echo "[3/4] Starting $LEAF_COUNT mock leaf nodes …"
$PYTHON -m rpi.mock_leaf \
    --leaves "$LEAF_COUNT" \
    --coordinators "$PEERS" \
    --interval 0.5 \
    > /tmp/echo-leaves.log 2>&1 &
echo $! >> "$PIDS_FILE"
sleep 1

# --------------------------------------------------------- dashboard
while lsof -nP -iTCP:"$DASH_PORT" -sTCP:LISTEN >/dev/null 2>&1; do
    if [ "$DASH_PORT" -ge 5010 ]; then
        echo "No free dashboard port found in 5000-5010."
        exit 1
    fi
    DASH_PORT=$((DASH_PORT + 1))
done

echo "[4/4] Starting dashboard on http://localhost:${DASH_PORT} …"
$PYTHON -m rpi.dashboard.app \
    --dash-port "$DASH_PORT" \
    > /tmp/echo-dashboard.log 2>&1 &
echo $! >> "$PIDS_FILE"

echo -n "Waiting for dashboard HTTP …"
for _ in $(seq 1 40); do
    if curl -sf "http://localhost:${DASH_PORT}/" >/dev/null 2>&1; then
        echo " ready."
        break
    fi
    sleep 0.25
    echo -n "."
done
echo ""

echo ""
echo "============================================"
echo "  Demo running!"
echo ""
echo "  Dashboard : http://localhost:${DASH_PORT}"
echo "  Logs      : /tmp/echo-coord-*.log"
echo "              /tmp/echo-leaves.log"
echo "              /tmp/echo-dashboard.log"
echo ""
echo "  Stop with : bash scripts/demo.sh stop"
echo "============================================"

# Open browser (macOS)
if command -v open >/dev/null 2>&1; then
    open "http://localhost:${DASH_PORT}"
fi
