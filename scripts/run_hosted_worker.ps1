$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot

$env:DISCORD_INGEST_ENABLED = "true"
$env:PARSER_WORKER_ENABLED = "true"
$env:STARTUP_BACKFILL_ENABLED = "true"
$env:STARTUP_BACKFILL_LOOKBACK_HOURS = "24"
$env:RUNTIME_NAME = "hosted_worker"
$env:RUNTIME_LABEL = "Hosted Worker"

Write-Host "Starting hosted worker process."
Write-Host "This process runs Discord ingest, backfill execution, and parser worker."
Write-Host "DATABASE_URL loaded from .env (not overridden)."

& "$repoRoot\.venv\Scripts\python.exe" -m app.discord.worker_service
