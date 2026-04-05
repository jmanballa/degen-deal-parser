$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$dbPath = Join-Path $repoRoot "data\degen_live.db"

Remove-Item Env:DATABASE_URL -ErrorAction SilentlyContinue

$env:DATABASE_URL = "sqlite:///" + ($dbPath -replace "\\", "/")
$env:DISCORD_INGEST_ENABLED = "false"
$env:PARSER_WORKER_ENABLED = "false"
$env:STARTUP_BACKFILL_ENABLED = "false"
$env:SESSION_HTTPS_ONLY = "false"
$env:SESSION_DOMAIN = ""
$env:RUNTIME_NAME = "local_web"
$env:RUNTIME_LABEL = "Local Web"
$env:WORKER_RUNTIME_NAME = "local_worker"
$env:WORKER_RUNTIME_LABEL = "Local Worker"

Write-Host "Starting local web-only host mode."
Write-Host "Discord ingest, backfill execution, and parser worker are disabled for this session."
Write-Host "Session cookie is set for localhost (HTTPS-only and domain overridden)."
Write-Host "Using local SQLite database at $dbPath"

& "$repoRoot\.venv\Scripts\python.exe" -m uvicorn app.main:app --host 127.0.0.1 --port 8000
