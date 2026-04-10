$ErrorActionPreference = "Stop"

# ── Config ──────────────────────────────────────────────────────────
$pgBin       = "C:\Program Files\PostgreSQL\17\bin"
$dbName      = "degen_live"
$dbUser      = "degen"
$dbHost      = "127.0.0.1"
$dbPort      = "5432"

$localBackupDir = "C:\backups\degen-db"
$keepLocal      = 7          # number of local backups to retain

# rclone remote name (set up via `rclone config`)
$rcloneRemote = "onedrive"
$remotePath   = "backups/degen-db"

$logFile = Join-Path $localBackupDir "backup.log"

# ── Helpers ─────────────────────────────────────────────────────────
function Write-Log {
    param([string]$Message)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "[$ts] $Message"
    Write-Host $line
    Add-Content -Path $logFile -Value $line
}

# ── Pre-flight ──────────────────────────────────────────────────────
if (-not (Test-Path $localBackupDir)) {
    New-Item -ItemType Directory -Path $localBackupDir -Force | Out-Null
}

if (-not (Get-Command rclone -ErrorAction SilentlyContinue)) {
    Write-Log "ERROR: rclone not found in PATH. Install with: winget install Rclone.Rclone"
    exit 1
}

$pgDump = Join-Path $pgBin "pg_dump.exe"
if (-not (Test-Path $pgDump)) {
    Write-Log "ERROR: pg_dump not found at $pgDump"
    exit 1
}

# ── Dump ────────────────────────────────────────────────────────────
$timestamp = Get-Date -Format "yyyy-MM-dd_HHmmss"
$dumpFile  = Join-Path $localBackupDir "degen_live_$timestamp.dump"

Write-Log "Starting pg_dump of $dbName ..."

$env:PGPASSWORD = "degen42069"
try {
    & $pgDump -h $dbHost -p $dbPort -U $dbUser -Fc -Z6 -f $dumpFile $dbName
    if ($LASTEXITCODE -ne 0) { throw "pg_dump exited with code $LASTEXITCODE" }
} finally {
    Remove-Item Env:\PGPASSWORD -ErrorAction SilentlyContinue
}

$sizeMB = [math]::Round((Get-Item $dumpFile).Length / 1MB, 2)
Write-Log "Dump complete: $dumpFile ($sizeMB MB)"

# ── Upload to OneDrive ───────────────────────────────────────────────
Write-Log "Uploading to ${rcloneRemote}:${remotePath}/ ..."
& rclone copy $dumpFile "${rcloneRemote}:${remotePath}/" --progress
if ($LASTEXITCODE -ne 0) {
    Write-Log "ERROR: rclone upload failed (exit code $LASTEXITCODE)"
    exit 1
}
Write-Log "Upload complete."

# ── Prune old local backups ─────────────────────────────────────────
$oldFiles = Get-ChildItem -Path $localBackupDir -Filter "degen_live_*.dump" |
    Sort-Object LastWriteTime -Descending |
    Select-Object -Skip $keepLocal

if ($oldFiles) {
    foreach ($f in $oldFiles) {
        Remove-Item $f.FullName -Force
        Write-Log "Pruned old backup: $($f.Name)"
    }
}

# ── Prune old remote backups (keep last 30) ─────────────────────────
Write-Log "Pruning remote backups older than 30 days ..."
& rclone delete "${rcloneRemote}:${remotePath}/" --min-age 30d
if ($LASTEXITCODE -ne 0) {
    Write-Log "WARNING: remote prune failed (non-fatal)"
}

Write-Log "Backup pipeline complete."
