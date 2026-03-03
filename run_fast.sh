#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

export RUN_STARTUP_MAINTENANCE="${RUN_STARTUP_MAINTENANCE:-0}"
export AUTO_SYNC_DEFAULT_WORKBOOKS="${AUTO_SYNC_DEFAULT_WORKBOOKS:-0}"
export AUTO_ENSURE_TEXT_INDEXES="${AUTO_ENSURE_TEXT_INDEXES:-0}"
export PERF_SQL_SLOW_MS="${PERF_SQL_SLOW_MS:-1200}"

exec flask --app app.py run --no-reload "$@"

