# Swap .env for local testing, restore on exit
Copy-Item .env .env.bak -Force
Copy-Item .env.local .env -Force

try {
    .venv\Scripts\uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
} finally {
    Copy-Item .env.bak .env -Force
    Remove-Item .env.bak -ErrorAction SilentlyContinue
    Write-Host "`n.env restored to production values."
}
