$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..")).Path
$backendScript = Join-Path $repoRoot "scripts\\run_backend.ps1"
$frontendScript = Join-Path $repoRoot "scripts\\run_frontend.ps1"
$setupScript = Join-Path $repoRoot "scripts\\setup.ps1"
$hasVenv = Test-Path (Join-Path $repoRoot ".venv")
$hasPortablePython = Test-Path (Join-Path $repoRoot ".python310\\python.exe")
$hasFrontendDeps = Test-Path (Join-Path $repoRoot "frontend\\node_modules")

if ((-not ($hasVenv -or $hasPortablePython)) -or -not $hasFrontendDeps) {
  Write-Host "First-time setup detected. Installing dependencies..."
  powershell -ExecutionPolicy Bypass -File $setupScript
}

$backendCmd = "& '$backendScript'"
$frontendCmd = "& '$frontendScript'"

function Get-ListeningPid([int]$port) {
  $conn = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($conn) { return $conn.OwningProcess }
  return $null
}

$backendExistingPid = Get-ListeningPid 8000
$frontendExistingPid = Get-ListeningPid 5173

foreach ($existingProcId in @($backendExistingPid, $frontendExistingPid)) {
  if ($existingProcId) {
    try {
      Write-Host "Stopping existing process on service port (PID: $existingProcId) to start latest code..."
      Stop-Process -Id $existingProcId -Force
    } catch {}
  }
}

Start-Sleep -Milliseconds 350
$backend = Start-Process -FilePath "powershell" -ArgumentList "-NoProfile", "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $backendCmd -WorkingDirectory $repoRoot -PassThru
$frontend = Start-Process -FilePath "powershell" -ArgumentList "-NoProfile", "-NoExit", "-ExecutionPolicy", "Bypass", "-Command", $frontendCmd -WorkingDirectory $repoRoot -PassThru

function Wait-HttpOk([string]$url, [int]$timeoutSeconds = 60) {
  $start = Get-Date
  while (((Get-Date) - $start).TotalSeconds -lt $timeoutSeconds) {
    try {
      $r = Invoke-WebRequest -UseBasicParsing -Uri $url -TimeoutSec 2
      if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) { return $true }
    } catch {}
    Start-Sleep -Milliseconds 500
  }
  return $false
}

$uiOk = Wait-HttpOk "http://127.0.0.1:5173" 90
if ($uiOk) {
  Start-Process "http://127.0.0.1:5173"
} else {
  Write-Host "UI not responding yet at http://127.0.0.1:5173"
  Write-Host "Check the Frontend window for errors (it should be open)."
}

Write-Host "Backend PID: $($backend.Id)"
Write-Host "Frontend PID: $($frontend.Id)"
Write-Host "Press Enter to stop launched windows from this runner (if still active)."
Read-Host | Out-Null

foreach ($p in @($backend, $frontend)) {
  try {
    if (-not $p.HasExited) { Stop-Process -Id $p.Id -Force }
  } catch {}
}
