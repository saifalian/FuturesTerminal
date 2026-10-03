param(
  [string]$ModelsRoot = "models/XRPUSDT",
  [string]$OutputJsonPath = ""
)

$ErrorActionPreference = 'Stop'
$runDirs = Get-ChildItem $ModelsRoot -Recurse -Directory | Where-Object { $_.Name -like 'run_*' } | Sort-Object LastWriteTime -Descending

$result = [ordered]@{}

function Write-Kv {
  param([string]$Key, $Value)
  if ($null -eq $Value) {
    Write-Output "$Key="
  } else {
    Write-Output "$Key=$Value"
  }
}

function Add-Result {
  param([string]$Key, $Value)
  $result[$Key] = $Value
  Write-Kv -Key $Key -Value $Value
}

function Save-ResultJson {
  if ($OutputJsonPath -and $OutputJsonPath.Trim() -ne "") {
    $outDir = Split-Path -Parent $OutputJsonPath
    if ($outDir -and -not (Test-Path $outDir)) { New-Item -ItemType Directory -Path $outDir | Out-Null }
    ($result | ConvertTo-Json -Depth 10) | Set-Content -Path $OutputJsonPath
    Add-Result -Key "output_json_path" -Value (Resolve-Path $OutputJsonPath)
  }
}

if (-not $runDirs -or $runDirs.Count -eq 0) {
  Add-Result -Key "status" -Value "no_runs_found"
  Save-ResultJson
  exit 0
}

$run = $runDirs[0].FullName
$metricsPath = Join-Path $run 'metrics.json'
$healthPath = Join-Path $run 'data_health_report.json'
$runInfoPath = Join-Path $run 'run_info.txt'

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

function Resolve-EvField {
  param(
    $RunInfo,
    [string]$FieldName
  )
  $top = $RunInfo.$FieldName
  if ($null -ne $top -and "$top" -ne '') { return $top }
  if ($RunInfo.ev_summary -and $RunInfo.ev_summary.PSObject.Properties.Name -contains $FieldName) {
    return $RunInfo.ev_summary.$FieldName
  }
  return $null
}

Add-Result -Key "status" -Value "ok"
Add-Result -Key "run_dir" -Value $run

if (Test-Path $metricsPath) {
  $m = Get-Content $metricsPath -Raw | ConvertFrom-Json
  Add-Result -Key "samples_test" -Value $m.samples_test
  Add-Result -Key "direction_accuracy" -Value $m.direction_accuracy
  Add-Result -Key "direction_f1_macro" -Value $m.direction_f1_macro
  Add-Result -Key "pnl_proxy" -Value $m.pnl_proxy
  Add-Result -Key "sharpe_proxy" -Value $m.sharpe_proxy
} else {
  Add-Result -Key "metrics_json" -Value "missing"
}

if (Test-Path $healthPath) {
  $h = Get-Content $healthPath -Raw | ConvertFrom-Json
  Add-Result -Key "data_health_ok" -Value $h.data_health_ok
  if ($h.coverage) {
    Add-Result -Key "coverage_hours" -Value $h.coverage.total_hours
    Add-Result -Key "gaps_gt_5s" -Value $h.coverage.gaps_gt_5s
  }
  if ($h.event_mix) {
    Add-Result -Key "event_mix_bookticker" -Value $h.event_mix.bookticker_count
    Add-Result -Key "event_mix_trade" -Value $h.event_mix.trade_count
    Add-Result -Key "event_mix_depth" -Value $h.event_mix.depth_count
  }
  if ($h.normalized_event_type_counts) {
    Add-Result -Key "normalized_event_mix_bookticker" -Value $h.normalized_event_type_counts.bookticker
    Add-Result -Key "normalized_event_mix_trade" -Value $h.normalized_event_type_counts.trade
    Add-Result -Key "normalized_event_mix_depth" -Value $h.normalized_event_type_counts.depth
    Add-Result -Key "event_mix_authoritative_source" -Value "normalized_event_type_counts"
  }
  if ($h.fail_reasons) {
    Add-Result -Key "fail_reasons_count" -Value $h.fail_reasons.Count
  }
} else {
  Add-Result -Key "data_health_report_json" -Value "missing"
}

$ri = Get-RunInfoJson -Path $runInfoPath
if ($null -eq $ri) {
  Add-Result -Key "run_info_json" -Value "parse_failed_or_missing"
} else {
  Add-Result -Key "final_test_avg_trade_return_after_cost" -Value $ri.final_test_avg_trade_return_after_cost
  Add-Result -Key "final_test_pnl_after_cost" -Value $ri.final_test_pnl_after_cost
  Add-Result -Key "final_test_reason" -Value $ri.final_test_reason
  Add-Result -Key "walk_forward_pass_count" -Value $ri.walk_forward_pass_count
  Add-Result -Key "long_allowed" -Value $ri.long_allowed
  Add-Result -Key "short_allowed" -Value $ri.short_allowed
  Add-Result -Key "recommended_live_mode" -Value $ri.recommended_live_mode
  Add-Result -Key "signal_quality_failed" -Value $ri.signal_quality_failed
  Add-Result -Key "avg_true_long_good_return_after_cost" -Value (Resolve-EvField -RunInfo $ri -FieldName 'avg_true_long_good_return_after_cost')
  Add-Result -Key "avg_true_short_good_return_after_cost" -Value (Resolve-EvField -RunInfo $ri -FieldName 'avg_true_short_good_return_after_cost')
  Add-Result -Key "avg_bad_long_return_after_cost" -Value (Resolve-EvField -RunInfo $ri -FieldName 'avg_bad_long_return_after_cost')
  Add-Result -Key "avg_bad_short_return_after_cost" -Value (Resolve-EvField -RunInfo $ri -FieldName 'avg_bad_short_return_after_cost')

  if ($ri.probability_margin_sweep) {
    Add-Result -Key "probability_margin_sweep_json" -Value ($ri.probability_margin_sweep | ConvertTo-Json -Compress)
  }
  if ($ri.top_k_selective_eval) {
    Add-Result -Key "top_k_selective_eval_json" -Value ($ri.top_k_selective_eval | ConvertTo-Json -Compress)
  }
  if ($ri.confidence_bucket_eval_after_cost) {
    Add-Result -Key "confidence_bucket_eval_after_cost_json" -Value ($ri.confidence_bucket_eval_after_cost | ConvertTo-Json -Compress)
  }
}

Save-ResultJson
