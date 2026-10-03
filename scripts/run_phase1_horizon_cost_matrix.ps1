param(
  [string]$BaseUrl = "http://127.0.0.1:8000",
  [string]$Pair = "XRPUSDT",
  [string]$DatasetSource = "local_market_events",
  [string]$OutputJson = "artifacts/phase1_horizon_cost_matrix.json",
  [string]$OutputCsv = "artifacts/phase1_horizon_cost_matrix.csv",
  [switch]$RunAll,
  [int]$ApiRetryCount = 6,
  [int]$ApiRetrySleepSeconds = 10,
  [int]$RunMaxWallMinutes = 180
)

$ErrorActionPreference = "Stop"

function Invoke-Api {
  param([string]$Method, [string]$Url, $Body = $null)
  $lastErr = $null
  for ($i = 0; $i -lt [Math]::Max(1, $ApiRetryCount); $i++) {
    try {
      if ($null -eq $Body) { return Invoke-RestMethod -Method $Method -Uri $Url -TimeoutSec 45 }
      return Invoke-RestMethod -Method $Method -Uri $Url -ContentType "application/json" -Body ($Body | ConvertTo-Json -Depth 30) -TimeoutSec 45
    } catch {
      $lastErr = $_
      Start-Sleep -Seconds $ApiRetrySleepSeconds
    }
  }
  throw $lastErr
}

function Load-Results([string]$Path) {
  if (!(Test-Path $Path)) { return @() }
  $raw = Get-Content $Path -Raw
  if ([string]::IsNullOrWhiteSpace($raw)) { return @() }
  $parsed = $raw | ConvertFrom-Json
  if ($parsed -is [System.Array]) { return @($parsed) }
  return @($parsed)
}

function Save-Results($Rows, [string]$JsonPath, [string]$CsvPath) {
  $jsonDir = Split-Path -Parent $JsonPath
  if ($jsonDir -and !(Test-Path $jsonDir)) { New-Item -ItemType Directory -Path $jsonDir | Out-Null }
  $csvDir = Split-Path -Parent $CsvPath
  if ($csvDir -and !(Test-Path $csvDir)) { New-Item -ItemType Directory -Path $csvDir | Out-Null }

  ($Rows | ConvertTo-Json -Depth 30) | Set-Content -Path $JsonPath
  $Rows |
    Select-Object run_id,max_hold_seconds,tp,sl,data_health_ok,final_test_avg_trade_return_after_cost,final_test_pnl_after_cost,final_test_reason,walk_forward_pass_count,long_allowed,short_allowed,recommended_live_mode,signal_quality_failed,approval_ok,fail_reason |
    Export-Csv -Path $CsvPath -NoTypeInformation
}

function Wait-Run([string]$RunId, [int]$PollSeconds = 20) {
  $started = Get-Date
  while ($true) {
    $elapsedMin = ((Get-Date) - $started).TotalMinutes
    if ($elapsedMin -gt $RunMaxWallMinutes) {
      throw "run_execution_timeout_exceeded_${RunMaxWallMinutes}m"
    }
    try {
      $run = Invoke-Api -Method "GET" -Url "$BaseUrl/ml/runs/$RunId"
      $status = "$($run.status)"
      Write-Output ("run={0} status={1} stage={2} progress={3}" -f $RunId, $status, $run.stage, $run.stage_progress)
      if ($status -in @("COMPLETED", "FAILED", "STOPPED", "PAUSED")) { return $run }
    } catch {
      Write-Output ("run={0} poll_retry reason={1}" -f $RunId, $_.Exception.Message)
    }
    Start-Sleep -Seconds $PollSeconds
  }
}

function Get-BestBucket($Rows) {
  if ($null -eq $Rows) { return $null }
  $best = $null
  foreach ($r in $Rows) {
    if ($null -eq $best -or [double]$r.avg_return_after_cost_pct -gt [double]$best.avg_return_after_cost_pct) { $best = $r }
  }
  return $best
}

$matrix = @()
foreach ($h in @(120, 240, 300, 600)) {
  foreach ($pairCfg in @(@{tp=0.10;sl=0.07}, @{tp=0.14;sl=0.08}, @{tp=0.20;sl=0.10})) {
    $matrix += [pscustomobject]@{ max_hold_seconds=$h; tp=$pairCfg.tp; sl=$pairCfg.sl; key=("{0}|{1}|{2}" -f $h,$pairCfg.tp,$pairCfg.sl) }
  }
}

$results = @(Load-Results -Path $OutputJson)
$doneKeys = @{}
foreach ($r in $results) { $doneKeys["$($r.max_hold_seconds)|$($r.tp)|$($r.sl)"] = $true }
$pending = @($matrix | Where-Object { -not $doneKeys.ContainsKey($_.key) })

if ($pending.Count -eq 0) {
  Write-Output "Phase 1 matrix already complete."
  exit 0
}

$toRun = if ($RunAll) { $pending } else { @($pending[0]) }
$profile = Invoke-Api -Method "GET" -Url "$BaseUrl/ml/pairs/$Pair/profile"

# Lock phase constraints.
$profile.target_mode = "trade_outcome"
$profile.training.model_type = "lstm_attention"
$profile.paper_bot.fee_bps_round_trip = 3.0
$profile.paper_bot.slippage_bps_round_trip = 1.0
$profile.paper_bot.safety_edge_buffer_bps = 1.5
$profile.paper_bot.ambiguity_margin_bps = 2.0
$profile.paper_bot.max_spread_bps = 4.0

foreach ($cfg in $toRun) {
  $runRow = [ordered]@{
    run_id = $null
    max_hold_seconds = [int]$cfg.max_hold_seconds
    tp = [double]$cfg.tp
    sl = [double]$cfg.sl
    data_health_ok = $null
    label_distribution = $null
    final_test_avg_trade_return_after_cost = $null
    final_test_pnl_after_cost = $null
    final_test_reason = $null
    best_probability_margin_bucket = $null
    best_top_k_bucket = $null
    best_confidence_bucket = $null
    walk_forward_pass_count = $null
    long_allowed = $null
    short_allowed = $null
    recommended_live_mode = $null
    signal_quality_failed = $null
    approval_ok = $false
    fail_reason = $null
  }

  try {
    $profile.paper_bot.max_hold_seconds = [int]$cfg.max_hold_seconds
    $profile.triple_barrier.tp_pct = [double]$cfg.tp
    $profile.triple_barrier.sl_pct = [double]$cfg.sl
    [void](Invoke-Api -Method "POST" -Url "$BaseUrl/ml/pairs/$Pair/profile" -Body $profile)

    $start = Invoke-Api -Method "POST" -Url "$BaseUrl/ml/runs/start" -Body @{ pair_symbol=$Pair; dataset_source=$DatasetSource }
    $runId = "$($start.run_id)"
    $runRow.run_id = $runId

    $done = Wait-Run -RunId $runId
    $m = $done.metrics

    $runRow.data_health_ok = $m.data_health_ok
    $runRow.label_distribution = $m.split_label_distribution
    $runRow.final_test_avg_trade_return_after_cost = $m.final_test_avg_trade_return_after_cost
    $runRow.final_test_pnl_after_cost = $m.final_test_pnl_after_cost
    $runRow.final_test_reason = $m.final_test_reason
    $runRow.best_probability_margin_bucket = Get-BestBucket $m.probability_margin_sweep
    $runRow.best_top_k_bucket = Get-BestBucket $m.top_k_selective_eval
    $runRow.best_confidence_bucket = Get-BestBucket $m.confidence_bucket_eval_after_cost
    $runRow.walk_forward_pass_count = $m.walk_forward_pass_count
    $runRow.long_allowed = $m.long_allowed
    $runRow.short_allowed = $m.short_allowed
    $runRow.recommended_live_mode = $m.recommended_live_mode
    $runRow.signal_quality_failed = $m.signal_quality_failed
    $runRow.approval_ok = [bool]($m.approval_ok)
    $runRow.fail_reason = if ($runRow.approval_ok) { $null } else { "$($m.approval_reason)" }

    if ($done.status -ne "COMPLETED" -and [string]::IsNullOrWhiteSpace($runRow.fail_reason)) {
      $runRow.fail_reason = "run_status_$($done.status)"
    }
  }
  catch {
    $runRow.approval_ok = $false
    if ($null -eq $runRow.run_id) {
      $runRow.run_id = "not_started"
      $runRow.fail_reason = "exception_before_run_start: $($_.Exception.Message)"
    } else {
      # Avoid mislabeling API timeout while run is still active.
      try {
        $probe = Invoke-Api -Method "GET" -Url "$BaseUrl/ml/runs/$($runRow.run_id)"
        if ("$($probe.status)" -in @("RUNNING","QUEUED")) {
          $runRow.fail_reason = "run_still_running_polling_timeout"
        } else {
          $runRow.fail_reason = "run_status_$($probe.status)"
        }
      } catch {
        $runRow.fail_reason = "exception_after_run_start: $($_.Exception.Message)"
      }
    }
  }
  finally {
    $results += [pscustomobject]$runRow
    Save-Results -Rows $results -JsonPath $OutputJson -CsvPath $OutputCsv
    Write-Output ("row_saved run_id={0} hold={1} tp={2} sl={3} fail_reason={4}" -f $runRow.run_id,$runRow.max_hold_seconds,$runRow.tp,$runRow.sl,$runRow.fail_reason)
  }
}

Write-Output ("Phase 1 step complete. rows_total={0} pending={1}" -f $results.Count, (@($matrix | Where-Object { -not ($results | Where-Object { ""+$_.max_hold_seconds+"|"+$_.tp+"|"+$_.sl -eq $_.key }) }).Count))
