$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$devDbPath = Join-Path $repoRoot "data\degen_ui_dev.db"
$seedDbPath = Join-Path $repoRoot "data\degen_live.db"

if (-not (Test-Path $devDbPath) -and (Test-Path $seedDbPath)) {
    Copy-Item $seedDbPath $devDbPath
}

$env:DATABASE_URL = "sqlite:///" + ($devDbPath -replace "\\", "/")
$env:DISCORD_INGEST_ENABLED = "false"
$env:PARSER_WORKER_ENABLED = "false"
$env:STARTUP_BACKFILL_ENABLED = "false"
$env:PUBLIC_BASE_URL = "http://127.0.0.1:8000"
$env:SESSION_HTTPS_ONLY = "false"
$env:SESSION_DOMAIN = ""
$env:AUTH_RESEED_PASSWORDS = "false"
$env:RUNTIME_NAME = "local_ui_dev"
$env:RUNTIME_LABEL = "UI Dev"

Write-Host "Starting fast local UI mode using $devDbPath"
Write-Host "Discord ingest and parser worker are disabled for this session."

& "$repoRoot\.venv\Scripts\python.exe" -m uvicorn app.main:app --reload
