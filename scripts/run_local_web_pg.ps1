$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot

Remove-Item Env:DATABASE_URL -ErrorAction SilentlyContinue

$env:DATABASE_URL = "postgresql+psycopg://degen:degen42069@100.110.34.106:5432/degen_live"
$env:DISCORD_INGEST_ENABLED = "false"
$env:PARSER_WORKER_ENABLED = "false"
$env:STARTUP_BACKFILL_ENABLED = "false"
$env:SESSION_HTTPS_ONLY = "false"
$env:SESSION_DOMAIN = "none"
$env:RUNTIME_NAME = "local_web_pg"
$env:RUNTIME_LABEL = "Local Web (Prod PG)"
$env:WORKER_RUNTIME_NAME = "local_worker"
$env:WORKER_RUNTIME_LABEL = "Local Worker"

Write-Host "Starting local web-only host mode with PRODUCTION PostgreSQL on Machine B."
Write-Host "Discord ingest, backfill execution, and parser worker are disabled for this session."
Write-Host "Session cookie is set for localhost (HTTPS-only and domain overridden)."
Write-Host "Database: PostgreSQL @ 100.110.34.106:5432/degen_live (via Tailscale)"

& "$repoRoot\.venv\Scripts\python.exe" -m uvicorn app.main:app --host 127.0.0.1 --port 8000
