#!/usr/bin/env bash
set -Eeuo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

export DISCORD_INGEST_ENABLED=true
export PARSER_WORKER_ENABLED=true
export STARTUP_BACKFILL_ENABLED=true
export STARTUP_BACKFILL_LOOKBACK_HOURS=24
export RUNTIME_NAME=hosted_worker
export RUNTIME_LABEL="Hosted Worker"

echo "Starting hosted worker process."
echo "This process runs Discord ingest, backfill execution, and parser worker."
echo "DATABASE_URL loaded from .env (not overridden)."

exec python -m app.discord.worker_service
