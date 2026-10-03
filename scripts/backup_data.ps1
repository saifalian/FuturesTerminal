$ErrorActionPreference = "Stop"

$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$target = "backend/data/backup_$timestamp"
New-Item -ItemType Directory -Path $target -Force | Out-Null

Copy-Item "backend/data/sqlite" -Destination "$target/sqlite" -Recurse -Force -ErrorAction SilentlyContinue
Copy-Item "backend/data/replays" -Destination "$target/replays" -Recurse -Force -ErrorAction SilentlyContinue
Copy-Item "backend/data/snapshots" -Destination "$target/snapshots" -Recurse -Force -ErrorAction SilentlyContinue

Write-Host "Backup created at $target"
