$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..")).Path
Set-Location $repoRoot

if (-not (Test-Path ".env") -and (Test-Path ".env.example")) {
  Copy-Item ".env.example" ".env"
}

if (Get-Command py -ErrorAction SilentlyContinue) {
  $pythonCmd = "py"
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
  $pythonCmd = "python"
} else {
  throw "Python was not found. Install Python 3.10+ and rerun setup."
}

if (-not (Test-Path ".venv")) {
  & $pythonCmd -m venv .venv
}

. .\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

Push-Location frontend
npm install
Pop-Location

Write-Host "Setup complete at $repoRoot"
