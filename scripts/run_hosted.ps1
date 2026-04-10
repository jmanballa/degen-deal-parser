$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$webScript = Join-Path $PSScriptRoot "run_hosted_web.ps1"
$workerScript = Join-Path $PSScriptRoot "run_hosted_worker.ps1"
$healthUrl = "http://127.0.0.1:8000/health"
$maxRestarts = 20
$restartCooldownSeconds = 15
$livenessCheckIntervalSeconds = 60
$livenessTimeoutSeconds = 15
$livenessFailuresBeforeRestart = 3

function Wait-ForHealth {
    param(
        [string]$Url,
        [int]$TimeoutSeconds = 90
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) {
                return $true
            }
        } catch {
            Start-Sleep -Seconds 2
        }
    }

    return $false
}

function Start-WebProcess {
    $proc = Start-Process `
        -FilePath "powershell.exe" `
        -ArgumentList @("-ExecutionPolicy", "Bypass", "-File", $webScript) `
        -WorkingDirectory $repoRoot `
        -PassThru
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Web process started (PID $($proc.Id))"
    return $proc
}

function Start-WorkerProcess {
    $proc = Start-Process `
        -FilePath "powershell.exe" `
        -ArgumentList @("-ExecutionPolicy", "Bypass", "-File", $workerScript) `
        -WorkingDirectory $repoRoot `
        -PassThru
    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Worker process started (PID $($proc.Id))"
    return $proc
}

Write-Host "Starting hosted web process first."
$webProcess = Start-WebProcess

Write-Host "Waiting for web health at $healthUrl"
if (-not (Wait-ForHealth -Url $healthUrl -TimeoutSeconds 90)) {
    try { Stop-Process -Id $webProcess.Id -Force } catch {}
    throw "Web process did not become healthy within 90 seconds."
}

Write-Host "Web is healthy. Starting worker process."
$workerProcess = Start-WorkerProcess

Write-Host "Web PID: $($webProcess.Id)  Worker PID: $($workerProcess.Id)"
Write-Host "Processes will auto-restart on crash (max $maxRestarts restarts)."
Write-Host "Press Ctrl+C in this window to stop both processes."

$webRestarts = 0
$workerRestarts = 0
$consecutiveLivenessFailures = 0
$lastLivenessCheck = [DateTime]::MinValue

function Test-Liveness {
    param([string]$Url, [int]$TimeoutSeconds = 15)
    try {
        $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec $TimeoutSeconds
        return ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500)
    } catch {
        return $false
    }
}

try {
    while ($true) {
        if ($webProcess.HasExited) {
            $webRestarts++
            $consecutiveLivenessFailures = 0
            Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Web process crashed (restart $webRestarts/$maxRestarts)"
            if ($webRestarts -ge $maxRestarts) {
                throw "Web process exceeded max restarts ($maxRestarts)."
            }
            Start-Sleep -Seconds $restartCooldownSeconds
            $webProcess = Start-WebProcess
            if (-not (Wait-ForHealth -Url $healthUrl -TimeoutSeconds 90)) {
                throw "Web process failed health check after restart."
            }
            Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Web process recovered."
        }

        # Liveness check: detect hung process (alive but not responding)
        $now = Get-Date
        if ((-not $webProcess.HasExited) -and (($now - $lastLivenessCheck).TotalSeconds -ge $livenessCheckIntervalSeconds)) {
            $lastLivenessCheck = $now
            if (Test-Liveness -Url $healthUrl -TimeoutSeconds $livenessTimeoutSeconds) {
                $consecutiveLivenessFailures = 0
            } else {
                $consecutiveLivenessFailures++
                Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Liveness check failed ($consecutiveLivenessFailures/$livenessFailuresBeforeRestart)"
                if ($consecutiveLivenessFailures -ge $livenessFailuresBeforeRestart) {
                    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Web process is hung - killing and restarting."
                    try { Stop-Process -Id $webProcess.Id -Force } catch {}
                    Start-Sleep -Seconds 3
                    $webRestarts++
                    $consecutiveLivenessFailures = 0
                    if ($webRestarts -ge $maxRestarts) {
                        throw "Web process exceeded max restarts ($maxRestarts)."
                    }
                    $webProcess = Start-WebProcess
                    if (-not (Wait-ForHealth -Url $healthUrl -TimeoutSeconds 90)) {
                        throw "Web process failed health check after restart."
                    }
                    Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Web process recovered from hang."
                }
            }
        }

        if ($workerProcess.HasExited) {
            $workerRestarts++
            Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Worker process crashed (restart $workerRestarts/$maxRestarts)"
            if ($workerRestarts -ge $maxRestarts) {
                throw "Worker process exceeded max restarts ($maxRestarts)."
            }
            Start-Sleep -Seconds $restartCooldownSeconds
            $workerProcess = Start-WorkerProcess
            Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] Worker process restarted."
        }
        Start-Sleep -Seconds 5
    }
} finally {
    foreach ($proc in @($workerProcess, $webProcess)) {
        try {
            if ($proc -and -not $proc.HasExited) {
                Stop-Process -Id $proc.Id -Force
            }
        } catch {}
    }
}
