#!/usr/bin/env bash
# ============================================================================
# DataFlow Automator - 1-Click Runner for Linux & macOS
# ============================================================================
set -e

if command -v node >/dev/null 2>&1; then
    echo "[RUN] Executing via Node.js runtime..."
    node automate.js "$@"
elif command -v python3 >/dev/null 2>&1; then
    echo "[RUN] Executing via Python 3 runtime..."
    python3 automate.py "$@"
elif command -v python >/dev/null 2>&1; then
    echo "[RUN] Executing via Python runtime..."
    python automate.py "$@"
else
    echo "[ERROR] Neither Node.js nor Python 3 was detected in your PATH."
    exit 1
fi

