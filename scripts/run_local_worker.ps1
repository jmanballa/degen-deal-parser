$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$dbPath = Join-Path $repoRoot "data\degen_live.db"

Remove-Item Env:DATABASE_URL -ErrorAction SilentlyContinue

$env:DATABASE_URL = "sqlite:///" + ($dbPath -replace "\\", "/")
$env:DISCORD_INGEST_ENABLED = "true"
$env:PARSER_WORKER_ENABLED = "true"
$env:STARTUP_BACKFILL_ENABLED = "true"
$env:STARTUP_BACKFILL_LOOKBACK_HOURS = "24"
$env:RUNTIME_NAME = "local_worker"
$env:RUNTIME_LABEL = "Local Worker"

Write-Host "Starting local worker host mode."
Write-Host "This process runs Discord ingest, backfill execution, and parser worker."
Write-Host "Using local SQLite database at $dbPath"

& "$repoRoot\.venv\Scripts\python.exe" -m app.discord.worker_service
