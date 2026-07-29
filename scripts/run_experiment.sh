#!/usr/bin/env bash
# Run one hardware-transport experiment: a full cluster over MQTT with a
# seeded workload, collecting sim-compatible metrics CSVs.
#
# Usage:
#   bash scripts/run_experiment.sh --protocol echod [--duration 30] \
#       [--seed 42] [--coordinators 5] [--leaves 10] \
#       [--partition-at 10 --heal-at 20] [--out results/hw]
#
# Artifacts (per run): results/hw/<protocol>/<seed>/
#   metrics.csv  nodes.csv  timeseries.csv  summary.json
#
# Works on localhost (one machine, mock batteries) and is the same entry
# point used on the Pi cluster — only the transport changes (real
# batteries + separate hosts), not the harness.
set -euo pipefail
cd "$(dirname "$0")/.."

# ------------------------------------------------------------ defaults
PROTOCOL=""
DURATION=30
SEED=42
COORDS=5
LEAVES=10
PARTITION_AT=""
HEAL_AT=""
OUT_BASE="results/hw"
BURST_INTERVAL=1.0
PY=python3

while [ $# -gt 0 ]; do
    case "$1" in
        --protocol) PROTOCOL="$2"; shift 2 ;;
        --duration) DURATION="$2"; shift 2 ;;
        --seed) SEED="$2"; shift 2 ;;
        --coordinators) COORDS="$2"; shift 2 ;;
        --leaves) LEAVES="$2"; shift 2 ;;
        --partition-at) PARTITION_AT="$2"; shift 2 ;;
        --heal-at) HEAL_AT="$2"; shift 2 ;;
        --burst-interval) BURST_INTERVAL="$2"; shift 2 ;;
        --out) OUT_BASE="$2"; shift 2 ;;
        *) echo "unknown arg: $1"; exit 1 ;;
    esac
done

case "$PROTOCOL" in
    raft|echo|echod) ;;
    *) echo "usage: $0 --protocol raft|echo|echod [--duration N] [--seed N]"; exit 1 ;;
esac

OUT_DIR="$OUT_BASE/$PROTOCOL/$SEED"
mkdir -p "$OUT_DIR"

PIDS=""
cleanup() {
    [ -n "$PIDS" ] && kill $PIDS 2>/dev/null || true
    [ -n "$PIDS" ] && wait $PIDS 2>/dev/null || true
}
trap cleanup EXIT

# ------------------------------------------------------------- broker
if command -v mosquitto >/dev/null 2>&1; then
    mosquitto -p 1883 -d 2>/dev/null || true
    sleep 1
fi

# ---------------------------------------------------------- collector
# Runs for the experiment window plus a margin to cover startup and the
# final status flush; writes CSVs and exits on its own.
COLLECTOR_DURATION=$(echo "$DURATION + 6" | bc)
$PY -m rpi.metrics_collector \
    --protocol "$PROTOCOL" --duration "$COLLECTOR_DURATION" \
    --seed "$SEED" --out "$OUT_DIR" \
    --coordinators "$COORDS" --leaves "$LEAVES" \
    ${PARTITION_AT:+--partition-at "$PARTITION_AT"} \
    ${HEAL_AT:+--heal-at "$HEAL_AT"} \
    > "$OUT_DIR/collector.log" 2>&1 &
COLLECTOR_PID=$!
PIDS="$COLLECTOR_PID"
sleep 1  # let the collector subscribe before nodes start publishing

# -------------------------------------------------------- coordinators
# Deterministic battery spread (95, 85, 75, ...) — same for every
# protocol so runs are comparable.
PEERS=""
for i in $(seq 0 $((COORDS - 1))); do
    [ -n "$PEERS" ] && PEERS="$PEERS,"
    PEERS="${PEERS}coord-${i}"
done

echo "== run_experiment: protocol=$PROTOCOL duration=${DURATION}s seed=$SEED coords=$COORDS leaves=$LEAVES"
for i in $(seq 0 $((COORDS - 1))); do
    BATT=$(( 95 - 10 * i ))
    $PY -m rpi.coordinator.echo_node \
        --node-id "coord-${i}" --peers "$PEERS" \
        --protocol "$PROTOCOL" --mock --battery "$BATT" \
        > "$OUT_DIR/coord-${i}.log" 2>&1 &
    PIDS="$PIDS $!"
done
sleep 2

# ------------------------------------------------------------- leaves
LEAF_EXTRA=""
[ "$PROTOCOL" = "echod" ] && LEAF_EXTRA="--edge-filter"
$PY -m rpi.mock_leaf \
    --leaves "$LEAVES" --coordinators "$PEERS" \
    --workload bursts --seed "$SEED" \
    --burst-interval "$BURST_INTERVAL" --start-delay 2.0 \
    --duration "$DURATION" --stats $LEAF_EXTRA \
    > "$OUT_DIR/leaves.log" 2>&1 &
LEAVES_PID=$!
PIDS="$PIDS $LEAVES_PID"

# ----------------------------------------------------- partition plan
if [ -n "$PARTITION_AT" ] && [ -n "$HEAL_AT" ]; then
    MID=$(( COORDS / 2 ))
    SIDE_A=""; SIDE_B=""
    for i in $(seq 0 $((MID - 1))); do
        [ -n "$SIDE_A" ] && SIDE_A="$SIDE_A,"
        SIDE_A="${SIDE_A}coord-${i}"
    done
    for i in $(seq "$MID" $((COORDS - 1))); do
        [ -n "$SIDE_B" ] && SIDE_B="$SIDE_B,"
        SIDE_B="${SIDE_B}coord-${i}"
    done
    (
        sleep "$PARTITION_AT"
        echo "== injecting partition: [$SIDE_A] | [$SIDE_B]"
        $PY -m rpi.partition_ctl partition --side "$SIDE_A" --side "$SIDE_B"
        sleep "$(echo "$HEAL_AT - $PARTITION_AT" | bc)"
        echo "== healing partition"
        $PY -m rpi.partition_ctl heal
    ) &
    PIDS="$PIDS $!"
fi

# -------------------------------------------------------------- wait
echo "== running ${DURATION}s (collector window ${COLLECTOR_DURATION}s) …"
wait "$LEAVES_PID" 2>/dev/null || true     # leaves stop at --duration
PIDS=$(echo "$PIDS" | sed "s/$LEAVES_PID//")
wait "$COLLECTOR_PID" 2>/dev/null || true  # then collector writes CSVs
PIDS=$(echo "$PIDS" | sed "s/$COLLECTOR_PID//")

echo ""
echo "== done. Artifacts in $OUT_DIR:"
echo "   $(cat "$OUT_DIR/metrics.csv" | tail -1)"
echo ""
echo "   metrics.csv  nodes.csv  timeseries.csv  summary.json"
