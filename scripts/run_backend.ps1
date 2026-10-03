$ErrorActionPreference = "Stop"

${scriptDir} = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..")).Path
Set-Location $repoRoot

if (-not (Test-Path ".venv")) {
  $portablePython = Join-Path $repoRoot ".python310\\python.exe"
  if (Test-Path $portablePython) {
    $env:PYTHONPATH = "backend"
    & $portablePython -m app.main
    exit $LASTEXITCODE
  }
  throw "Missing .venv in $repoRoot and no portable Python found at .python310\\python.exe. Run .\\scripts\\setup.ps1 first."
}

. (Join-Path $repoRoot ".venv\\Scripts\\Activate.ps1")
$env:PYTHONPATH = "backend"
python -m app.main
