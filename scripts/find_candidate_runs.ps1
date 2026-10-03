param(
  [string]$ModelsRoot = "models",
  [int]$Limit = 200
)

$ErrorActionPreference = 'Stop'

function Get-RunInfoJson {
  param([string]$Path)
  if (-not (Test-Path $Path)) { return $null }
  $text = Get-Content $Path -Raw
  $matches = [regex]::Matches($text, '\{')
  if ($matches.Count -eq 0) { return $null }
  for ($i = $matches.Count - 1; $i -ge 0; $i--) {
    $start = $matches[$i].Index
    $candidate = $text.Substring($start).Trim()
    try {
      $obj = $candidate | ConvertFrom-Json -ErrorAction Stop
      if ($obj.PSObject.Properties.Name -contains 'signal_quality_failed') {
        return $obj
      }
    } catch {
      continue
    }
  }
  return $null
}

function To-Num($v) {
  if ($null -eq $v -or "$v" -eq '') { return [double]::NaN }
  try { return [double]$v } catch { return [double]::NaN }
}

$runDirs = Get-ChildItem $ModelsRoot -Recurse -Directory | Where-Object { $_.Name -like 'run_*' } | Sort-Object LastWriteTime -Descending
if (-not $runDirs -or $runDirs.Count -eq 0) {
  Write-Output "status=no_runs_found"
  exit 0
}

$rows = New-Object System.Collections.Generic.List[object]
foreach ($runDir in ($runDirs | Select-Object -First $Limit)) {
  $runInfoPath = Join-Path $runDir.FullName 'run_info.txt'
  $ri = Get-RunInfoJson -Path $runInfoPath
  if ($null -eq $ri) { continue }

  $avg = To-Num $ri.final_test_avg_trade_return_after_cost
  $pnl = To-Num $ri.final_test_pnl_after_cost
  $wf = To-Num $ri.walk_forward_pass_count
  $longAllowed = [bool]$ri.long_allowed
  $shortAllowed = [bool]$ri.short_allowed
  $signalFailed = [bool]$ri.signal_quality_failed
  $reason = "$($ri.final_test_reason)"

  $isCandidate = (
    -not [double]::IsNaN($avg) -and $avg -gt 0 -and
    -not [double]::IsNaN($pnl) -and $pnl -gt 0 -and
    -not [double]::IsNaN($wf) -and $wf -ge 1 -and
    ($longAllowed -or $shortAllowed) -and
    (-not $signalFailed)
  )

  $rows.Add([pscustomobject]@{
    run_dir = $runDir.FullName
    updated_local = $runDir.LastWriteTime.ToString('yyyy-MM-dd HH:mm:ss zzz')
    final_test_avg_trade_return_after_cost = $ri.final_test_avg_trade_return_after_cost
    final_test_pnl_after_cost = $ri.final_test_pnl_after_cost
    walk_forward_pass_count = $ri.walk_forward_pass_count
    long_allowed = $ri.long_allowed
    short_allowed = $ri.short_allowed
    signal_quality_failed = $ri.signal_quality_failed
    final_test_reason = $reason
    recommended_live_mode = $ri.recommended_live_mode
    candidate = $isCandidate
  })
}

if ($rows.Count -eq 0) {
  Write-Output "status=no_parseable_runs"
  exit 0
}

$candidates = $rows | Where-Object { $_.candidate }
$top = $rows | Sort-Object @{Expression = {[double]$_.final_test_avg_trade_return_after_cost}; Descending = $true}, @{Expression = {[double]$_.final_test_pnl_after_cost}; Descending = $true} | Select-Object -First 10

Write-Output "status=ok"
Write-Output "runs_scanned=$($rows.Count)"
Write-Output "candidates_found=$($candidates.Count)"

if ($candidates.Count -gt 0) {
  Write-Output "best_candidate_run=$($candidates[0].run_dir)"
}

Write-Output "top10_json=$($top | ConvertTo-Json -Compress)"
if ($candidates.Count -gt 0) {
  Write-Output "candidates_json=$($candidates | Select-Object -First 10 | ConvertTo-Json -Compress)"
}
