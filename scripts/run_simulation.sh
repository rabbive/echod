#!/usr/bin/env bash
# Run the Phase 1 simulation (ECHO vs Raft) with sensible defaults.
# Usage:  bash scripts/run_simulation.sh [extra args...]
#
# Examples:
#   bash scripts/run_simulation.sh --charts
#   bash scripts/run_simulation.sh --partition-at 2 --heal-at 4 --charts

set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== ECHO Phase 1 Simulation ==="
python -m simulation.main \
    --coordinators 5 \
    --leaves 10 \
    --duration 6 \
    --battery-drain 0.01 \
    "$@"
