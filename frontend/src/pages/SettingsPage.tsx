import { useEffect, useMemo, useRef, useState, type ChangeEvent } from "react";
import type { RecordingMode, WorkloadControls } from "../App";
import {
  activatePreset,
  getActiveWorkspacePresetValues,
  loadWorkspacePresetStore,
  removePreset,
  saveWorkspacePresetStore,
  upsertPreset,
  WorkspacePresetStore,
  WorkspacePresetValues,
} from "../app/workspacePreset";
import BotTradeMap, { type BotInferencePoint, type BotTradeMapCandle } from "../components/ml/BotTradeMap";

const TIMEFRAME_OPTIONS = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h"];
const ML_UI_STORAGE_KEY = "ml_trainer_ui_v1";
const ML_INSTANCES_CACHE_KEY = "ml_instances_cache_v1";
const SETTINGS_SESSION_UI_KEY = "settings_ui_session_v1";
const API_BASE = "http://127.0.0.1:8000";
export type SettingsView = "hub" | "workspace_presets" | "trainer_bot" | "service_settings" | "storage_management";

type ExchangeOption = { exchange_id: string; name: string };

type MlProfile = {
  pair_symbol: string;
  selected_exchanges: string[];
  training_selected_exchanges: string[];
  bot_data_selected_exchanges: string[];
  bot_execution_selected_exchanges: string[];
  bot_signal_mode: "combined" | "majority" | "best";
  execution_position_mode: "combined_position" | "separate_positions";
  use_local_data: boolean;
  use_historic_data: boolean;
  historic_data_days: number;
  historic_data_days_max?: number;
  horizons: number[];
  label_threshold_pct: number;
  min_data_hours: number;
  normalizer_window: number;
  replay_candle_limit: number;
  training_hour_window_enabled: boolean;
  training_hour_start: number;
  training_hour_end: number;
  full_data_mode: boolean;
  strict_full_windows_mode: boolean;
  label_guard_enabled?: boolean;
  target_mode?: "triple_barrier" | "fixed_return" | "trade_outcome";
  triple_barrier?: {
    tp_pct: number;
    sl_pct: number;
    timeout_steps: number;
  };
  barrier_debug?: {
    enable_future_path_probe: boolean;
    future_path_probe_samples: number;
    future_path_probe_depth: number;
    step_contiguity_tolerance_ms: number;
    timeout_unit_hint: "steps" | "seconds_hint";
  };
  training: {
    epochs: number;
    batch_size: number;
    learning_rate: number;
    lookback_steps: number;
    hidden_size: number;
    num_layers: number;
    dropout: number;
    warm_start_from_current: boolean;
    use_class_weighted_loss: boolean;
    class_weight_mode: "auto" | "manual";
    class_weight_down: number;
    class_weight_flat: number;
    class_weight_up: number;
    use_focal_loss: boolean;
    focal_gamma: number;
    focal_use_alpha_class_weights: boolean;
    use_balanced_sampler: boolean;
    use_class_quota_batches: boolean;
    class_quota_per_batch: number;
    label_smoothing: number;
    epoch_trace_capture_list: string;
    capture_trace_train: boolean;
    capture_trace_val: boolean;
    capture_trace_test: boolean;
    early_stopping_monitor: "val_loss" | "macro_f1";
    early_stopping_enabled: boolean;
    early_stopping_patience: number;
    early_stopping_min_delta: number;
  };
  paper_bot: {
    enabled: boolean;
    gate_mode?: "confidence_only" | "combined";
    min_flat_rate_required: number;
    max_label_price_rejected_rate: number;
    confidence_threshold: number;
    entry_confidence_threshold?: number;
    exit_confidence_threshold?: number;
    minimum_directional_edge?: number;
    temperature_scaling_enabled?: boolean;
    minimum_action_rate?: number;
    minimum_directional_samples?: number;
    estimated_roundtrip_cost_bps?: number;
    max_signal_age_ms?: number;
    volatility_guard_enabled?: boolean;
    max_realized_volatility?: number;
    volatility_guard_action?: "hold_only" | "raise_threshold_multiplier";
    max_spread_bps?: number;
    min_orderbook_depth?: number;
    live_calibration_guard_action?: "monitor_only" | "block_entries";
    max_trades_per_hour?: number;
    max_consecutive_entries?: number;
    min_seconds_between_entries?: number;
    quality_threshold: number;
    poll_delay_ms: number;
    max_position_qty: number;
    max_hold_seconds: number;
    leverage: number;
    commission_fee_pct: number;
    one_trade_at_time: boolean;
    initial_balance: number;
    take_profit_pct: number;
    stop_loss_pct: number;
    use_trailing_stop: boolean;
    trailing_stop_pct: number;
    regime_filter_enabled: boolean;
    max_spread_pct: number;
    min_trade_rate_10s: number;
    min_confidence_gap: number;
  };
  updated_at: string;
};

type DataStatus = {
  pair_symbol: string;
  file_count: number;
  rows_total: number;
  min_ts_ms: number | null;
  max_ts_ms: number | null;
  data_hours: number;
  total_data_hours_available?: number;
  window_anchor_min_ts_ms?: number | null;
  window_anchor_max_ts_ms?: number | null;
  hour_slot_count?: number;
  selected_source?: "features_manifest" | "local_market_events";
  selected_status?: {
    file_count: number;
    rows_total: number;
    min_ts_ms: number | null;
    max_ts_ms: number | null;
    data_hours: number;
  };
  active_source?: string;
  active_source_path?: string;
  use_local_data?: boolean;
  use_historic_data?: boolean;
  historic_data_days?: number;
  mode?: string;
  selected_exchanges?: string[];
  exchange_progress?: BackfillExchangeProgress[];
  live_status?: {
    file_count: number;
    rows_total: number;
    min_ts_ms: number | null;
    max_ts_ms: number | null;
    data_hours: number;
  };
  historic_status?: {
    file_count: number;
    rows_total: number;
    min_ts_ms: number | null;
    max_ts_ms: number | null;
    data_hours: number;
  };
};

type MlRun = {
  run_id: string;
  pair_symbol: string;
  status: "IDLE" | "RUNNING" | "PAUSED" | "STOPPED" | "COMPLETED" | "FAILED" | "QUEUED";
  stage: "queued" | "data" | "features" | "labeling" | "training" | "evaluation" | "ready" | "failed" | "stopped";
  stage_progress: number;
  device: string;
  created_at: string;
  updated_at: string;
  started_at: string | null;
  ended_at: string | null;
  config: Record<string, unknown>;
  metrics: Record<string, unknown>;
  error_text: string;
};

type MlRunLog = {
  ts: string;
  level: string;
  message: string;
};

type MlBotStatus = {
  pair_symbol: string;
  status: string;
  position_side: string;
  entry_price: number;
  qty: number;
  unrealized_pnl: number;
  realized_pnl: number;
  initial_balance?: number;
  current_balance?: number;
  equity?: number;
  bot_data_source?: string;
  snapshot_freshness_sec?: number | null;
  active_exchange_count?: number;
  active_session_id?: string;
  last_signal: Record<string, unknown>;
  updated_at: string;
};

type MlBotTrade = {
  trade_id: string;
  instance_id: string;
  session_id?: string;
  pair_symbol: string;
  side: string;
  qty: number;
  leverage: number;
  commission_fee_pct: number;
  entry_price: number;
  exit_price: number;
  entry_notional: number;
  exit_notional: number;
  entry_fee: number;
  exit_fee: number;
  gross_pnl: number;
  net_pnl: number;
  opened_at: string;
  closed_at: string;
  duration_seconds: number;
  close_reason: string;
  decision_context?: Record<string, unknown>;
  created_at: string;
};

type BotTradeMapCandlesResponse = {
  instance_id: string;
  pair_symbol: string;
  source?: string;
  candles?: BotTradeMapCandle[];
  inference_points?: BotInferencePoint[];
  reason?: string;
};

type MlBotTradeSession = {
  session_id: string;
  instance_id: string;
  name: string;
  is_default: boolean;
  created_at: string;
  updated_at: string;
  archived: boolean;
  trade_count: number;
};

type BackfillExchangeProgress = {
  exchange_id: string;
  exchange_name?: string;
  status: string;
  rows: number;
  data_hours: number;
  reason: string;
  features_available?: {
    ohlcv?: boolean;
    trades?: boolean;
    funding?: boolean;
    liquidations?: boolean;
  };
  resolved_symbol?: string | null;
};

type BackfillJobStatus = {
  job_id: string;
  pair_symbol: string;
  status: string;
  stage: string;
  progress: number;
  mode?: string;
  source_path: string;
  target_path: string;
  days: number;
  selected_exchanges?: string[];
  exchange_progress?: BackfillExchangeProgress[];
  started_at: string;
  updated_at: string;
  ended_at: string | null;
  error_text: string;
  log_count?: number;
  latest_log?: MlRunLog | null;
};

type RunActionName = "start" | "pause" | "stop";

type RunActionProgress = {
  action: RunActionName;
  startedAtMs: number;
  etaMs: number;
  state: "active" | "done";
};

type SavedMlUi = {
  selectedPair?: string;
  selectedInstanceId?: string;
  selectedRunId?: string;
};

type MlInstance = {
  instance_id: string;
  name: string;
  pair_symbol: string;
  created_at: string;
  updated_at: string;
  archived: boolean;
  selected_exchange_count?: number;
  latest_approved_metrics?: Record<string, unknown>;
  bot_status?: string;
};

type StorageEntry = {
  entry_id: string;
  category_id: string;
  pair_symbol: string;
  name: string;
  type: "file" | "folder" | "db_group" | "parquet_chunk";
  path: string;
  size_bytes: number;
  updated_at: string;
  delete_target: Record<string, unknown>;
};

type StorageCategory = {
  category_id: string;
  label: string;
  entry_count: number;
  size_bytes: number;
  entries: StorageEntry[];
};

type StorageOverview = {
  totals: {
    category_count: number;
    entry_count: number;
    size_bytes: number;
  };
  categories: StorageCategory[];
};

type StorageSubView = "storage_hub" | "storage_classic" | "storage_per_pair" | "storage_pair_detail";

type PairDataKindId =
  | "live_recording_files_replays"
  | "market_event_full_fidelity_chunks"
  | "live_raw_events_terminal_rows"
  | "training_features_disk"
  | "models_disk"
  | "ml_runs"
  | "trace_frames"
  | "bot_trades"
  | "bot_sessions"
  | "acquired_historic_data"
  | "historic_manifests";

type PairDataKindItem = {
  kind: PairDataKindId;
  label: string;
  sizeBytes: number;
  dbScoped?: boolean;
  entries: StorageEntry[];
};

type PairStorageSummary = {
  pairSymbol: string;
  totalSizeBytes: number;
  kinds: PairDataKindItem[];
};

type TrainingDatasetSource = "features_manifest" | "local_market_events";
type ToolMode = "live" | "replay" | "recording" | "training" | "bot";
type ToolRowId =
  | "book"
  | "candles"
  | "trades"
  | "mark_funding"
  | "liquidations"
  | "replay_candle_core"
  | "data_quality_score"
  | "weighted_exchange_controls"
  | "liquidity_event_alerts"
  | "signal_confluence_meter"
  | "execution_simulator_panel"
  | "confluence_score"
  | "long_short_checklist"
  | "entry_quality_meter"
  | "regime_detector"
  | "absorption_vs_breakout"
  | "spoof_confidence_score"
  | "mtf_alignment"
  | "playbook_tags"
  | "model_inputs_momentum"
  | "model_inputs_volatility"
  | "model_inputs_trend"
  | "model_inputs_orderflow"
  | "model_inputs_liquidity"
  | "model_inputs_context"
  | "main_chart"
  | "orderbook_dominance"
  | "live_ladder"
  | "live_ladder_depth"
  | "cvd_panel"
  | "liquidity_area"
  | "market_snapshot"
  | "bias_meter"
  | "positioning_panel"
  | "spoofing_panel"
  | "absorption_panel"
  | "safety_panel";
type ToolModeMatrix = {
  version: number;
  rows: Record<ToolRowId, Record<ToolMode, boolean>>;
};

const TOOL_ROW_META: Array<{ id: ToolRowId; label: string }> = [
  { id: "book", label: "Book/Spread" },
  { id: "candles", label: "Candles" },
  { id: "trades", label: "Trades" },
  { id: "mark_funding", label: "Mark/Funding" },
  { id: "liquidations", label: "Liquidations" },
  { id: "replay_candle_core", label: "Replay Candle Core" },
  { id: "data_quality_score", label: "Data Quality Score" },
  { id: "weighted_exchange_controls", label: "Weighted Exchange Controls" },
  { id: "liquidity_event_alerts", label: "Liquidity Event Alerts" },
  { id: "signal_confluence_meter", label: "Signal Confluence Meter" },
  { id: "execution_simulator_panel", label: "Execution Simulator Panel" },
  { id: "confluence_score", label: "Confluence Score (0-100)" },
  { id: "long_short_checklist", label: "Long / Short Checklist" },
  { id: "entry_quality_meter", label: "Entry Quality Meter" },
  { id: "regime_detector", label: "Regime Detector" },
  { id: "absorption_vs_breakout", label: "Absorption vs Breakout" },
  { id: "spoof_confidence_score", label: "Spoof Confidence Score" },
  { id: "mtf_alignment", label: "Multi-Timeframe Alignment" },
  { id: "playbook_tags", label: "Playbook Tags" },
  { id: "model_inputs_momentum", label: "Model Inputs: Momentum" },
  { id: "model_inputs_volatility", label: "Model Inputs: Volatility" },
  { id: "model_inputs_trend", label: "Model Inputs: Trend" },
  { id: "model_inputs_orderflow", label: "Model Inputs: Orderflow" },
  { id: "model_inputs_liquidity", label: "Model Inputs: Liquidity" },
  { id: "model_inputs_context", label: "Model Inputs: Context" },
  { id: "main_chart", label: "Main Chart" },
  { id: "orderbook_dominance", label: "Orderbook Dominance" },
  { id: "live_ladder", label: "Live Ladder" },
  { id: "live_ladder_depth", label: "Live Ladder Depth" },
  { id: "cvd_panel", label: "CVD Panel" },
  { id: "liquidity_area", label: "Liquidity Area" },
  { id: "market_snapshot", label: "Market Snapshot" },
  { id: "bias_meter", label: "Bias Meter" },
  { id: "positioning_panel", label: "Positioning Panel" },
  { id: "spoofing_panel", label: "Spoofing Panel" },
  { id: "absorption_panel", label: "Absorption Panel" },
  { id: "safety_panel", label: "Safety Panel" },
];

const TOOL_MODE_COLUMNS: ToolMode[] = ["live", "replay", "recording", "training", "bot"];

const DEFAULT_TOOL_MODE_MATRIX: ToolModeMatrix = {
  version: 1,
  rows: {
    book: { live: true, replay: true, recording: true, training: true, bot: true },
    candles: { live: true, replay: true, recording: true, training: true, bot: true },
    trades: { live: true, replay: true, recording: true, training: true, bot: true },
    mark_funding: { live: true, replay: true, recording: true, training: true, bot: true },
    liquidations: { live: true, replay: true, recording: true, training: true, bot: true },
    replay_candle_core: { live: true, replay: true, recording: true, training: true, bot: true },
    data_quality_score: { live: true, replay: true, recording: true, training: true, bot: true },
    weighted_exchange_controls: { live: true, replay: true, recording: true, training: true, bot: true },
    liquidity_event_alerts: { live: true, replay: true, recording: true, training: true, bot: true },
    signal_confluence_meter: { live: true, replay: true, recording: true, training: true, bot: true },
    execution_simulator_panel: { live: true, replay: true, recording: true, training: true, bot: true },
    confluence_score: { live: true, replay: true, recording: true, training: true, bot: true },
    long_short_checklist: { live: true, replay: true, recording: true, training: true, bot: true },
    entry_quality_meter: { live: true, replay: true, recording: true, training: true, bot: true },
    regime_detector: { live: true, replay: true, recording: true, training: true, bot: true },
    absorption_vs_breakout: { live: true, replay: true, recording: true, training: true, bot: true },
    spoof_confidence_score: { live: true, replay: true, recording: true, training: true, bot: true },
    mtf_alignment: { live: true, replay: true, recording: true, training: true, bot: true },
    playbook_tags: { live: true, replay: true, recording: true, training: true, bot: true },
    model_inputs_momentum: { live: true, replay: true, recording: true, training: true, bot: true },
    model_inputs_volatility: { live: true, replay: true, recording: true, training: true, bot: true },
    model_inputs_trend: { live: true, replay: true, recording: true, training: true, bot: true },
    model_inputs_orderflow: { live: true, replay: true, recording: true, training: true, bot: true },
    model_inputs_liquidity: { live: true, replay: true, recording: true, training: true, bot: true },
    model_inputs_context: { live: true, replay: true, recording: true, training: true, bot: true },
    main_chart: { live: true, replay: true, recording: true, training: true, bot: true },
    orderbook_dominance: { live: true, replay: true, recording: true, training: true, bot: true },
    live_ladder: { live: true, replay: true, recording: true, training: true, bot: true },
    live_ladder_depth: { live: true, replay: true, recording: true, training: true, bot: true },
    cvd_panel: { live: true, replay: true, recording: true, training: true, bot: true },
    liquidity_area: { live: true, replay: true, recording: true, training: true, bot: true },
    market_snapshot: { live: true, replay: true, recording: true, training: true, bot: true },
    bias_meter: { live: true, replay: true, recording: true, training: true, bot: true },
    positioning_panel: { live: true, replay: true, recording: true, training: true, bot: true },
    spoofing_panel: { live: true, replay: true, recording: true, training: true, bot: true },
    absorption_panel: { live: true, replay: true, recording: true, training: true, bot: true },
    safety_panel: { live: true, replay: true, recording: true, training: true, bot: true },
  },
};

type SettingsSessionUi = {
  storageSubView?: StorageSubView;
  selectedStoragePair?: string;
  trainingDatasetSource?: TrainingDatasetSource;
  mlDetailsOpen?: boolean;
};

function loadSettingsSessionUi(): SettingsSessionUi {
  try {
    const raw = window.sessionStorage.getItem(SETTINGS_SESSION_UI_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw) as SettingsSessionUi;
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

const RUN_ACTION_ETA_MS: Record<RunActionName, number> = {
  start: 6000,
  pause: 3000,
  stop: 3500,
};

const DEFAULT_ML_PROFILE: MlProfile = {
  pair_symbol: "",
  selected_exchanges: [],
  training_selected_exchanges: [],
  bot_data_selected_exchanges: [],
  bot_execution_selected_exchanges: [],
  bot_signal_mode: "combined",
  execution_position_mode: "combined_position",
  use_local_data: true,
  use_historic_data: false,
  historic_data_days: 90,
  historic_data_days_max: 3650,
  horizons: [5, 15, 30, 60],
  label_threshold_pct: 0.05,
  min_data_hours: 48,
  normalizer_window: 1000,
  replay_candle_limit: 240,
    training_hour_window_enabled: false,
    training_hour_start: 0,
    training_hour_end: 0,
    full_data_mode: true,
  strict_full_windows_mode: true,
  label_guard_enabled: true,
  target_mode: "trade_outcome",
  triple_barrier: {
    tp_pct: 0.08,
    sl_pct: 0.05,
    timeout_steps: 180,
  },
  barrier_debug: {
    enable_future_path_probe: true,
    future_path_probe_samples: 5,
    future_path_probe_depth: 20,
    step_contiguity_tolerance_ms: 500,
    timeout_unit_hint: "steps",
  },
  training: {
    epochs: 8,
    batch_size: 64,
    learning_rate: 0.001,
    lookback_steps: 60,
    hidden_size: 128,
    num_layers: 2,
    dropout: 0.2,
    warm_start_from_current: false,
    use_class_weighted_loss: true,
    class_weight_mode: "auto",
    class_weight_down: 1,
    class_weight_flat: 1,
    class_weight_up: 1,
    use_focal_loss: false,
    focal_gamma: 2,
    focal_use_alpha_class_weights: true,
    use_balanced_sampler: false,
    use_class_quota_batches: false,
    class_quota_per_batch: 4,
    label_smoothing: 0,
    epoch_trace_capture_list: "",
    capture_trace_train: false,
    capture_trace_val: false,
    capture_trace_test: false,
    early_stopping_monitor: "val_loss",
    early_stopping_enabled: false,
    early_stopping_patience: 1,
    early_stopping_min_delta: 0,
  },
  paper_bot: {
    enabled: false,
    gate_mode: "confidence_only",
    min_flat_rate_required: 0.12,
    max_label_price_rejected_rate: 0.005,
    confidence_threshold: 0.72,
    entry_confidence_threshold: 0.72,
    exit_confidence_threshold: 0.55,
    minimum_directional_edge: 0.03,
    temperature_scaling_enabled: true,
    minimum_action_rate: 0.03,
    minimum_directional_samples: 30,
    estimated_roundtrip_cost_bps: 6,
    max_signal_age_ms: 2000,
    volatility_guard_enabled: false,
    max_realized_volatility: 0.02,
    volatility_guard_action: "hold_only",
    max_spread_bps: 15,
    min_orderbook_depth: 0,
    live_calibration_guard_action: "monitor_only",
    max_trades_per_hour: 30,
    max_consecutive_entries: 5,
    min_seconds_between_entries: 5,
    quality_threshold: 0.72,
    poll_delay_ms: 2000,
    max_position_qty: 1,
    max_hold_seconds: 120,
    leverage: 5,
    commission_fee_pct: 0.04,
    one_trade_at_time: true,
    initial_balance: 100,
    take_profit_pct: 0.25,
    stop_loss_pct: 0.2,
    use_trailing_stop: false,
    trailing_stop_pct: 0.15,
    regime_filter_enabled: true,
    max_spread_pct: 0.15,
    min_trade_rate_10s: 3,
    min_confidence_gap: 0.06,
  },
  updated_at: "",
};

function cloneValues(values: WorkspacePresetValues): WorkspacePresetValues {
  return {
    timeframe: values.timeframe,
    showHeatmap: values.showHeatmap,
    thresholdLow: values.thresholdLow,
    thresholdHigh: values.thresholdHigh,
    rowSizeMultiplier: values.rowSizeMultiplier,
    heatmapCoverageScale: values.heatmapCoverageScale,
  };
}

function normalizePair(raw: string): string {
  return raw.trim().toUpperCase();
}

function loadMlUi(): SavedMlUi {
  try {
    const raw = window.localStorage.getItem(ML_UI_STORAGE_KEY);
    if (!raw) return {};
    return JSON.parse(raw) as SavedMlUi;
  } catch {
    return {};
  }
}

function formatDurationMs(ms: number): string {
  if (!Number.isFinite(ms) || ms <= 0) return "0s";
  const totalSeconds = Math.floor(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

function formatBytes(bytes: number): string {
  const value = Number.isFinite(bytes) ? Math.max(0, bytes) : 0;
  if (value < 1024) return `${value} B`;
  if (value < 1024 ** 2) return `${(value / 1024).toFixed(1)} KB`;
  if (value < 1024 ** 3) return `${(value / (1024 ** 2)).toFixed(2)} MB`;
  return `${(value / (1024 ** 3)).toFixed(2)} GB`;
}

function mergeProfile(pair: string, incoming: Partial<MlProfile> | null | undefined): MlProfile {
  if (!incoming) {
    return { ...DEFAULT_ML_PROFILE, pair_symbol: pair };
  }
  const historicDaysMax = Number.isFinite(Number(incoming.historic_data_days_max))
    ? Math.max(1, Math.round(Number(incoming.historic_data_days_max)))
    : 3650;
  const historicDays = Number.isFinite(Number(incoming.historic_data_days))
    ? Math.max(1, Math.min(historicDaysMax, Math.round(Number(incoming.historic_data_days))))
    : Math.min(90, historicDaysMax);
  return {
    pair_symbol: normalizePair(incoming.pair_symbol ?? pair),
    selected_exchanges: Array.isArray(incoming.selected_exchanges)
      ? incoming.selected_exchanges.map((item) => String(item).toLowerCase())
      : [],
    training_selected_exchanges: Array.isArray(incoming.training_selected_exchanges)
      ? incoming.training_selected_exchanges.map((item) => String(item).toLowerCase())
      : (Array.isArray(incoming.selected_exchanges) ? incoming.selected_exchanges.map((item) => String(item).toLowerCase()) : []),
    bot_data_selected_exchanges: Array.isArray(incoming.bot_data_selected_exchanges)
      ? incoming.bot_data_selected_exchanges.map((item) => String(item).toLowerCase())
      : (Array.isArray(incoming.selected_exchanges) ? incoming.selected_exchanges.map((item) => String(item).toLowerCase()) : []),
    bot_execution_selected_exchanges: Array.isArray(incoming.bot_execution_selected_exchanges)
      ? incoming.bot_execution_selected_exchanges.map((item) => String(item).toLowerCase())
      : (Array.isArray(incoming.selected_exchanges) ? incoming.selected_exchanges.map((item) => String(item).toLowerCase()) : []),
    bot_signal_mode:
      incoming.bot_signal_mode === "majority" || incoming.bot_signal_mode === "best"
        ? incoming.bot_signal_mode
        : "combined",
    execution_position_mode:
      incoming.execution_position_mode === "separate_positions"
        ? "separate_positions"
        : "combined_position",
    use_local_data: Boolean(incoming.use_local_data ?? true),
    use_historic_data: Boolean(incoming.use_historic_data ?? false),
    historic_data_days: historicDays,
    historic_data_days_max: historicDaysMax,
    horizons: Array.isArray(incoming.horizons) ? incoming.horizons.map((item) => Number(item)).filter((item) => Number.isFinite(item) && item > 0) : [5, 15, 30, 60],
    label_threshold_pct: Number.isFinite(Number(incoming.label_threshold_pct)) ? Number(incoming.label_threshold_pct) : 0.05,
    min_data_hours: Number.isFinite(Number(incoming.min_data_hours)) ? Number(incoming.min_data_hours) : 48,
    normalizer_window: Number.isFinite(Number(incoming.normalizer_window)) ? Number(incoming.normalizer_window) : 1000,
    replay_candle_limit: Number.isFinite(Number(incoming.replay_candle_limit)) ? Math.max(30, Math.min(5000, Number(incoming.replay_candle_limit))) : 240,
    training_hour_window_enabled: Boolean(incoming.training_hour_window_enabled ?? false),
    training_hour_start: Number.isFinite(Number(incoming.training_hour_start)) ? Math.max(0, Math.round(Number(incoming.training_hour_start))) : 0,
    training_hour_end: Number.isFinite(Number(incoming.training_hour_end)) ? Math.max(0, Math.round(Number(incoming.training_hour_end))) : 0,
    full_data_mode: Boolean(incoming.full_data_mode ?? true),
    strict_full_windows_mode: Boolean(incoming.strict_full_windows_mode ?? true),
    label_guard_enabled: Boolean(incoming.label_guard_enabled ?? true),
    target_mode: (() => {
      const mode = String(incoming.target_mode ?? "trade_outcome").toLowerCase();
      if (mode === "fixed_return") return "fixed_return";
      if (mode === "trade_outcome") return "trade_outcome";
      return "triple_barrier";
    })(),
    triple_barrier: {
      tp_pct: Number.isFinite(Number((incoming as Record<string, unknown>).triple_barrier && (incoming as Record<string, unknown>).triple_barrier && ((incoming as Record<string, unknown>).triple_barrier as Record<string, unknown>).tp_pct))
        ? Number(((incoming as Record<string, unknown>).triple_barrier as Record<string, unknown>).tp_pct)
        : 0.08,
      sl_pct: Number.isFinite(Number((incoming as Record<string, unknown>).triple_barrier && ((incoming as Record<string, unknown>).triple_barrier as Record<string, unknown>).sl_pct))
        ? Number(((incoming as Record<string, unknown>).triple_barrier as Record<string, unknown>).sl_pct)
        : 0.05,
      timeout_steps: Number.isFinite(Number((incoming as Record<string, unknown>).triple_barrier && ((incoming as Record<string, unknown>).triple_barrier as Record<string, unknown>).timeout_steps))
        ? Math.max(1, Math.round(Number(((incoming as Record<string, unknown>).triple_barrier as Record<string, unknown>).timeout_steps)))
        : 180,
    },
    barrier_debug: {
      enable_future_path_probe: Boolean(
        ((incoming as Record<string, unknown>).barrier_debug &&
          ((incoming as Record<string, unknown>).barrier_debug as Record<string, unknown>).enable_future_path_probe) ??
          true,
      ),
      future_path_probe_samples: Number.isFinite(Number((incoming as Record<string, unknown>).barrier_debug && ((incoming as Record<string, unknown>).barrier_debug as Record<string, unknown>).future_path_probe_samples))
        ? Math.max(1, Math.min(20, Math.round(Number(((incoming as Record<string, unknown>).barrier_debug as Record<string, unknown>).future_path_probe_samples))))
        : 5,
      future_path_probe_depth: Number.isFinite(Number((incoming as Record<string, unknown>).barrier_debug && ((incoming as Record<string, unknown>).barrier_debug as Record<string, unknown>).future_path_probe_depth))
        ? Math.max(5, Math.min(200, Math.round(Number(((incoming as Record<string, unknown>).barrier_debug as Record<string, unknown>).future_path_probe_depth))))
        : 20,
      step_contiguity_tolerance_ms: Number.isFinite(Number((incoming as Record<string, unknown>).barrier_debug && ((incoming as Record<string, unknown>).barrier_debug as Record<string, unknown>).step_contiguity_tolerance_ms))
        ? Math.max(1, Math.min(60000, Math.round(Number(((incoming as Record<string, unknown>).barrier_debug as Record<string, unknown>).step_contiguity_tolerance_ms))))
        : 500,
      timeout_unit_hint: String((incoming as Record<string, unknown>).barrier_debug && ((incoming as Record<string, unknown>).barrier_debug as Record<string, unknown>).timeout_unit_hint || "steps").toLowerCase() === "seconds_hint" ? "seconds_hint" : "steps",
    },
    training: {
      ...DEFAULT_ML_PROFILE.training,
      ...(incoming.training ?? {}),
      class_weight_mode:
        String((incoming.training as Record<string, unknown> | undefined)?.class_weight_mode ?? "auto").toLowerCase() === "manual"
          ? "manual"
          : "auto",
      early_stopping_monitor:
        String((incoming.training as Record<string, unknown> | undefined)?.early_stopping_monitor ?? "val_loss").toLowerCase() === "macro_f1"
          ? "macro_f1"
          : "val_loss",
    },
    paper_bot: {
      ...DEFAULT_ML_PROFILE.paper_bot,
      ...(incoming.paper_bot ?? {}),
    },
    updated_at: String(incoming.updated_at ?? ""),
  };
}

function normalizeToolModeMatrix(input: unknown): ToolModeMatrix {
  const rowsRaw =
    input && typeof input === "object" && "rows" in (input as Record<string, unknown>)
      ? ((input as Record<string, unknown>).rows as Record<string, unknown>)
      : {};
  const rows = { ...DEFAULT_TOOL_MODE_MATRIX.rows };
  for (const row of TOOL_ROW_META) {
    const rowRaw = rowsRaw && typeof rowsRaw === "object" ? (rowsRaw as Record<string, unknown>)[row.id] : undefined;
    const nextRow = { ...rows[row.id] };
    if (rowRaw && typeof rowRaw === "object") {
      for (const mode of TOOL_MODE_COLUMNS) {
        if (mode in (rowRaw as Record<string, unknown>)) {
          nextRow[mode] = Boolean((rowRaw as Record<string, unknown>)[mode]);
        }
      }
    }
    rows[row.id] = nextRow;
  }
  return { version: 1, rows };
}

function stageState(run: MlRun | null, stage: string): "done" | "active" | "pending" | "failed" {
  if (!run) return "pending";
  if (run.status === "FAILED") {
    return run.stage === stage ? "failed" : "pending";
  }
  const order = ["queued", "data", "features", "labeling", "training", "evaluation", "ready"];
  const targetIndex = order.indexOf(stage);
  const runIndex = order.indexOf(run.stage);
  if (run.status === "COMPLETED" && stage !== "queued") return "done";
  if (targetIndex === -1) return "pending";
  if (runIndex === targetIndex) return "active";
  if (runIndex > targetIndex) return "done";
  return "pending";
}

type SettingsPageProps = {
  workloadControls?: WorkloadControls;
  onWorkloadControlsChange?: (next: WorkloadControls) => void;
  recordingMode?: RecordingMode;
  onRecordingModeChange?: (mode: RecordingMode) => void;
  settingsView: SettingsView;
  onSettingsViewChange: (view: SettingsView) => void;
};

type WorkloadToggleKey =
  | "live_streaming_enabled"
  | "replay_enabled"
  | "training_enabled"
  | "bots_enabled"
  | "backfill_enabled";

export default function SettingsPage({
  workloadControls,
  onWorkloadControlsChange,
  recordingMode,
  onRecordingModeChange,
  settingsView,
  onSettingsViewChange,
}: SettingsPageProps) {
  const settingsSessionUi = useMemo(() => loadSettingsSessionUi(), []);
  const [presetStore, setPresetStore] = useState<WorkspacePresetStore>(() => loadWorkspacePresetStore());
  const [newPresetName, setNewPresetName] = useState("");
  const [status, setStatus] = useState("");
  const activeValues = useMemo(() => {
    const current = presetStore.presets[presetStore.activePreset];
    return cloneValues(current ?? getActiveWorkspacePresetValues());
  }, [presetStore]);
  const [draft, setDraft] = useState<WorkspacePresetValues>(activeValues);

  const savedMlUi = useMemo(() => loadMlUi(), []);
  const [trainerPairs, setTrainerPairs] = useState<string[]>([]);
  const [instances, setInstances] = useState<MlInstance[]>([]);
  const [selectedInstanceId, setSelectedInstanceId] = useState<string>(savedMlUi.selectedInstanceId ?? "");
  const [selectedPair, setSelectedPair] = useState<string>(savedMlUi.selectedPair ?? "");
  const [pairInput, setPairInput] = useState<string>("");
  const [selectedRunId, setSelectedRunId] = useState<string>(savedMlUi.selectedRunId ?? "");
  const [logsClearedRunId, setLogsClearedRunId] = useState<string>("");
  const logsClearedRunIdRef = useRef<string>("");
  const [runs, setRuns] = useState<MlRun[]>([]);
  const [runDetails, setRunDetails] = useState<MlRun | null>(null);
  const [runLogs, setRunLogs] = useState<MlRunLog[]>([]);
  const [dataStatus, setDataStatus] = useState<DataStatus | null>(null);
  const [botStatus, setBotStatus] = useState<MlBotStatus | null>(null);
  const [botTrades, setBotTrades] = useState<MlBotTrade[]>([]);
  const [botSessions, setBotSessions] = useState<MlBotTradeSession[]>([]);
  const [activeBotSessionId, setActiveBotSessionId] = useState<string>("");
  const [botExportScope, setBotExportScope] = useState<"selected" | "all">("selected");
  const [profileDraft, setProfileDraft] = useState<MlProfile | null>(null);
  const [profileSavedSnapshot, setProfileSavedSnapshot] = useState<string>("");
  const [profileModalOpen, setProfileModalOpen] = useState<boolean>(false);
  const [profileModalSection, setProfileModalSection] = useState<"pair" | "bot" | "trace" | "exp">("pair");
  const profileImportInputRef = useRef<HTMLInputElement | null>(null);
  const [exchangeOptions, setExchangeOptions] = useState<ExchangeOption[]>([]);
  const [trainerBusy, setTrainerBusy] = useState<boolean>(false);
  const [trainingDatasetSource, setTrainingDatasetSource] = useState<TrainingDatasetSource>(
    settingsSessionUi.trainingDatasetSource === "local_market_events" ? "local_market_events" : "features_manifest"
  );
  const [instancesLoading, setInstancesLoading] = useState<boolean>(false);
  const [trainerStatus, setTrainerStatus] = useState<string>("");
  const [trainerError, setTrainerError] = useState<string>("");
  const [storageOverview, setStorageOverview] = useState<StorageOverview | null>(null);
  const [storageBusy, setStorageBusy] = useState<boolean>(false);
  const [storageLoading, setStorageLoading] = useState<boolean>(false);
  const [storageError, setStorageError] = useState<string>("");
  const [storageStatus, setStorageStatus] = useState<string>("");
  const [storageSelectedIds, setStorageSelectedIds] = useState<Set<string>>(new Set());
  const [storageDeleteOpen, setStorageDeleteOpen] = useState<boolean>(false);
  const [storageDeleteTargets, setStorageDeleteTargets] = useState<StorageEntry[]>([]);
  const [storageSubView, setStorageSubView] = useState<StorageSubView>(() => {
    const raw = settingsSessionUi.storageSubView;
    return raw === "storage_classic" || raw === "storage_per_pair" || raw === "storage_pair_detail" ? raw : "storage_hub";
  });
  const [selectedStoragePair, setSelectedStoragePair] = useState<string>(settingsSessionUi.selectedStoragePair ?? "");
  const [pairDataSelectedKinds, setPairDataSelectedKinds] = useState<Set<PairDataKindId>>(new Set());
  const [storageDeleteJobId, setStorageDeleteJobId] = useState<string>("");
  const [storageDeleteProgress, setStorageDeleteProgress] = useState<string>("");
  const [isDocumentHidden, setIsDocumentHidden] = useState<boolean>(typeof document !== "undefined" ? document.hidden : false);
  const [mlDetailsOpen, setMlDetailsOpen] = useState<boolean>(Boolean(settingsSessionUi.mlDetailsOpen));
  const [runActionsOpen, setRunActionsOpen] = useState<boolean>(false);
  const [backfillJobId, setBackfillJobId] = useState<string>("");
  const [backfillStatus, setBackfillStatus] = useState<BackfillJobStatus | null>(null);
  const [backfillLogs, setBackfillLogs] = useState<MlRunLog[]>([]);
  const [runActionProgress, setRunActionProgress] = useState<RunActionProgress | null>(null);
  const [botExportMenuOpen, setBotExportMenuOpen] = useState<boolean>(false);
  const [botSessionCreateOpen, setBotSessionCreateOpen] = useState<boolean>(false);
  const [botSessionRenameOpen, setBotSessionRenameOpen] = useState<boolean>(false);
  const [botSessionInput, setBotSessionInput] = useState<string>("");
  const [runActionNowMs, setRunActionNowMs] = useState<number>(Date.now());
  const [stopRequestedRunId, setStopRequestedRunId] = useState<string>("");
  const [autoStopSuppressed, setAutoStopSuppressed] = useState<boolean>(false);
  const [botMapCandles, setBotMapCandles] = useState<BotTradeMapCandle[]>([]);
  const [botMapInferencePoints, setBotMapInferencePoints] = useState<BotInferencePoint[]>([]);
  const [botMapLoading, setBotMapLoading] = useState<boolean>(false);
  const [botMapError, setBotMapError] = useState<string>("");
  const [serviceSettingsStatus, setServiceSettingsStatus] = useState<string>("");
  const [serviceSettingsError, setServiceSettingsError] = useState<string>("");
  const [compactionBusy, setCompactionBusy] = useState<boolean>(false);
  const [toolModeMatrixSaved, setToolModeMatrixSaved] = useState<ToolModeMatrix>(DEFAULT_TOOL_MODE_MATRIX);
  const [toolModeMatrixDraft, setToolModeMatrixDraft] = useState<ToolModeMatrix>(DEFAULT_TOOL_MODE_MATRIX);
  const [toolModeMatrixApplying, setToolModeMatrixApplying] = useState<boolean>(false);
  const [recordingSoftCapBlockedPairs, setRecordingSoftCapBlockedPairs] = useState<string[]>([]);
  const [recordingEnabled, setRecordingEnabled] = useState<boolean>(false);
  const [recordingToggleBusy, setRecordingToggleBusy] = useState<boolean>(false);
  const [recordingSoftCapOverrideBusyPair, setRecordingSoftCapOverrideBusyPair] = useState<string>("");
  const botMapTradeFingerprintRef = useRef<string>("");
  const refreshLightInFlightRef = useRef<boolean>(false);
  const runPollInFlightRef = useRef<boolean>(false);
  const backfillPollInFlightRef = useRef<boolean>(false);
  const schedulerNextAtRef = useRef<{ light: number; run: number; runLogs: number; backfill: number }>({
    light: 0,
    run: 0,
    runLogs: 0,
    backfill: 0,
  });

  useEffect(() => {
    setDraft(activeValues);
  }, [activeValues]);

  useEffect(() => {
    const payload: SettingsSessionUi = {
      storageSubView,
      selectedStoragePair,
      trainingDatasetSource,
      mlDetailsOpen,
    };
    try {
      window.sessionStorage.setItem(SETTINGS_SESSION_UI_KEY, JSON.stringify(payload));
    } catch {
      // ignore session storage errors
    }
  }, [storageSubView, selectedStoragePair, trainingDatasetSource, mlDetailsOpen]);


  useEffect(() => {
    const payload: SavedMlUi = { selectedPair, selectedInstanceId, selectedRunId };
    window.localStorage.setItem(ML_UI_STORAGE_KEY, JSON.stringify(payload));
  }, [selectedPair, selectedInstanceId, selectedRunId]);

  useEffect(() => {
    void loadInstances();
    void loadExchangeCatalog();
  }, []);

  useEffect(() => {
    if (settingsView !== "trainer_bot") return;
    void loadInstances();
  }, [settingsView]);

  useEffect(() => {
    if (!selectedInstanceId) return;
    const current = instances.find((it) => it.instance_id === selectedInstanceId);
    if (!current) return;
    const pair = normalizePair(current.pair_symbol);
    if (pair !== selectedPair) {
      setSelectedPair(pair);
      return;
    }
    void loadPairWorkspace(pair, selectedInstanceId);
  }, [selectedInstanceId, instances]);

  useEffect(() => {
    if (!selectedPair) return;
    if (!selectedInstanceId) {
      void loadPairWorkspace(selectedPair);
    }
  }, [selectedPair, selectedInstanceId]);

  useEffect(() => {
    if (selectedInstanceId) return;
    setBotMapCandles([]);
    setBotMapInferencePoints([]);
    setBotMapError("");
    setBotMapLoading(false);
    botMapTradeFingerprintRef.current = "";
  }, [selectedInstanceId]);

  useEffect(() => {
    const onVisibility = () => setIsDocumentHidden(document.hidden);
    document.addEventListener("visibilitychange", onVisibility);
    return () => document.removeEventListener("visibilitychange", onVisibility);
  }, []);

  useEffect(() => {
    if (!workloadControls) return;
    if (workloadControls.bots_enabled) return;
    // Avoid showing stale bot-action messages while Bot Engine is disabled.
    setTrainerStatus((prev) => (prev.toLowerCase().includes("paper bot") ? "" : prev));
  }, [workloadControls?.bots_enabled]);

  useEffect(() => {
    if (!selectedRunId) {
      setRunDetails(null);
      setRunLogs([]);
    }
  }, [selectedRunId]);

  useEffect(() => {
    setLogsClearedRunId("");
    logsClearedRunIdRef.current = "";
  }, [selectedRunId]);

  useEffect(() => {
    if (!selectedPair) return;
    if (selectedInstanceId && !activeBotSessionId) return;
    void loadBotTrades(selectedPair, selectedInstanceId || undefined, activeBotSessionId || undefined, "selected");
  }, [activeBotSessionId, selectedPair, selectedInstanceId]);

  useEffect(() => {
    if (!runActionProgress || runActionProgress.state !== "active") return;
    setRunActionNowMs(Date.now());
    const timer = window.setInterval(() => {
      setRunActionNowMs(Date.now());
    }, 150);
    return () => window.clearInterval(timer);
  }, [runActionProgress]);

  const backfillIsTerminal = useMemo(() => {
    const status = String(backfillStatus?.status ?? "").toUpperCase();
    return status === "COMPLETED" || status === "FAILED" || status === "STOPPED";
  }, [backfillStatus?.status]);

  useEffect(() => {
    if (!backfillJobId) {
      setBackfillLogs([]);
    }
  }, [backfillJobId]);

  useEffect(() => {
    if (settingsView !== "trainer_bot") return;
    if (!selectedPair) return;
    schedulerNextAtRef.current = { light: 0, run: 0, runLogs: 0, backfill: 0 };

    const tick = () => {
      const now = Date.now();
      const nextAt = schedulerNextAtRef.current;

      const lowLoadAll = workloadControls
        ? !workloadControls.training_enabled && !workloadControls.bots_enabled && !workloadControls.backfill_enabled
        : false;
      const lightBase = lowLoadAll ? 12000 : 3000;
      const lightPollMs = isDocumentHidden ? Math.max(15000, lightBase * 2) : lightBase;
      if (now >= nextAt.light && !refreshLightInFlightRef.current) {
        nextAt.light = now + lightPollMs;
        refreshLightInFlightRef.current = true;
        void refreshLight(selectedPair, selectedInstanceId || undefined).finally(() => {
          refreshLightInFlightRef.current = false;
        });
      }

      if (selectedRunId) {
        const lowLoadRun = workloadControls ? !workloadControls.training_enabled : false;
        const runBase = lowLoadRun ? 12000 : 2200;
        const runPollMs = isDocumentHidden ? Math.max(12000, runBase * 2) : runBase;
        const runLogsPollMs = isDocumentHidden ? 20000 : 8000;
        if (now >= nextAt.run && !runPollInFlightRef.current) {
          nextAt.run = now + runPollMs;
          runPollInFlightRef.current = true;
          const runTasks: Array<Promise<unknown>> = [loadRunDetails(selectedRunId)];
          if (!isDocumentHidden && now >= nextAt.runLogs) {
            nextAt.runLogs = now + runLogsPollMs;
            runTasks.push(loadRunLogs(selectedRunId));
          }
          Promise.all(runTasks).finally(() => {
            runPollInFlightRef.current = false;
          });
        }
      }

      if (
        backfillJobId
        && !backfillIsTerminal
        && !(workloadControls && !workloadControls.backfill_enabled)
      ) {
        const backfillPollMs = isDocumentHidden ? 8000 : 2500;
        if (now >= nextAt.backfill && !backfillPollInFlightRef.current) {
          nextAt.backfill = now + backfillPollMs;
          backfillPollInFlightRef.current = true;
          Promise.all([loadBackfillStatus(backfillJobId), loadBackfillLogs(backfillJobId)]).finally(() => {
            backfillPollInFlightRef.current = false;
          });
        }
      }
    };

    tick();
    const timer = window.setInterval(tick, 1500);
    return () => window.clearInterval(timer);
  }, [
    settingsView,
    selectedPair,
    selectedInstanceId,
    selectedRunId,
    backfillJobId,
    backfillIsTerminal,
    workloadControls,
    isDocumentHidden,
    trainingDatasetSource,
  ]);

  const allExchangeOptions = useMemo(() => {
    const base = [...exchangeOptions];
    const byId = new Map(base.map((item) => [item.exchange_id, item]));
    for (const exchangeId of [
      ...(profileDraft?.training_selected_exchanges ?? []),
      ...(profileDraft?.bot_data_selected_exchanges ?? []),
      ...(profileDraft?.bot_execution_selected_exchanges ?? []),
    ]) {
      if (!byId.has(exchangeId)) {
        byId.set(exchangeId, {
          exchange_id: exchangeId,
          name: exchangeId.toUpperCase(),
        });
      }
    }
    return [...byId.values()].sort((a, b) => a.name.localeCompare(b.name));
  }, [
    exchangeOptions,
    profileDraft?.training_selected_exchanges,
    profileDraft?.bot_data_selected_exchanges,
    profileDraft?.bot_execution_selected_exchanges,
  ]);

  const profileDirty = useMemo(() => {
    if (!profileDraft) return false;
    return JSON.stringify(profileDraft) !== profileSavedSnapshot;
  }, [profileDraft, profileSavedSnapshot]);

  const selectedBotSession = useMemo(
    () => botSessions.find((item) => item.session_id === activeBotSessionId) ?? null,
    [botSessions, activeBotSessionId],
  );
  const totalBotSessionTrades = useMemo(
    () => botSessions.reduce((sum, item) => sum + Number(item.trade_count || 0), 0),
    [botSessions],
  );
  const lowLoadDisabledServices = useMemo(() => {
    if (!workloadControls) return [] as string[];
    const disabled: string[] = [];
    if (!workloadControls.live_streaming_enabled) disabled.push("Live Streaming");
    if (!workloadControls.replay_enabled) disabled.push("Replay Engine");
    if (!workloadControls.training_enabled) disabled.push("Training Jobs");
    if (!workloadControls.bots_enabled) disabled.push("Bot Engine");
    if (!workloadControls.backfill_enabled) disabled.push("Backfill Jobs");
    return disabled;
  }, [workloadControls]);

  function persist(nextStore: WorkspacePresetStore, nextStatus: string) {
    const saved = saveWorkspacePresetStore(nextStore);
    setPresetStore(saved);
    setStatus(nextStatus);
  }

  function handleActivate(name: string) {
    const nextStore = activatePreset(name);
    setPresetStore(nextStore);
    setDraft(cloneValues(nextStore.presets[nextStore.activePreset]));
    setStatus(`Activated preset: ${name}`);
  }

  function handleSaveActive() {
    const nextStore: WorkspacePresetStore = {
      ...presetStore,
      presets: {
        ...presetStore.presets,
        [presetStore.activePreset]: cloneValues(draft),
      },
    };
    persist(nextStore, `Saved preset: ${presetStore.activePreset}`);
  }

  function handleCreatePreset() {
    const name = newPresetName.trim();
    if (!name) {
      setStatus("Enter a preset name first.");
      return;
    }
    const nextStore = upsertPreset(name, draft);
    setPresetStore(nextStore);
    setDraft(cloneValues(nextStore.presets[nextStore.activePreset]));
    setNewPresetName("");
    setStatus(`Created preset: ${name}`);
  }

  function handleDeletePreset(name: string) {
    const nextStore = removePreset(name);
    setPresetStore(nextStore);
    setDraft(cloneValues(nextStore.presets[nextStore.activePreset]));
    setStatus(`Deleted preset: ${name}`);
  }

  async function loadPairs() {
    try {
      const response = await fetch(`${API_BASE}/ml/pairs`);
      const data = (await response.json()) as { pairs?: string[] };
      const pairs = Array.isArray(data.pairs) ? data.pairs.map((item) => normalizePair(String(item))).filter(Boolean) : [];
      setTrainerPairs(pairs);
      if (!pairs.length) {
        setSelectedPair("");
        return;
      }
      setSelectedPair((prev) => {
        if (prev && pairs.includes(prev)) return prev;
        if (savedMlUi.selectedPair && pairs.includes(normalizePair(savedMlUi.selectedPair))) {
          return normalizePair(savedMlUi.selectedPair);
        }
        return pairs[0];
      });
    } catch {
      setTrainerError("Could not load ML pairs.");
    }
  }

  async function loadInstances() {
    setInstancesLoading(true);
    try {
      const response = await fetch(`${API_BASE}/ml/instances?lite=1`);
      const payload = (await response.json()) as { instances?: MlInstance[] };
      const items = Array.isArray(payload.instances) ? payload.instances : [];
      setInstances(items);
      try {
        window.localStorage.setItem(ML_INSTANCES_CACHE_KEY, JSON.stringify(items));
      } catch {
        // ignore cache write failures
      }
      const pairs = Array.from(new Set(items.map((it) => normalizePair(it.pair_symbol)))).filter(Boolean);
      setTrainerPairs(pairs);
      if (!items.length) {
        setSelectedInstanceId("");
        setSelectedPair("");
        return;
      }
      setSelectedInstanceId((prev) => {
        if (prev && items.some((it) => it.instance_id === prev)) return prev;
        if (savedMlUi.selectedInstanceId && items.some((it) => it.instance_id === savedMlUi.selectedInstanceId)) {
          return savedMlUi.selectedInstanceId;
        }
        return items[0].instance_id;
      });
    } catch {
      let cached: MlInstance[] = [];
      try {
        const raw = window.localStorage.getItem(ML_INSTANCES_CACHE_KEY);
        const parsed = raw ? (JSON.parse(raw) as MlInstance[]) : [];
        cached = Array.isArray(parsed) ? parsed : [];
      } catch {
        cached = [];
      }
      if (cached.length) {
        setInstances(cached);
        const pairs = Array.from(new Set(cached.map((it) => normalizePair(it.pair_symbol)))).filter(Boolean);
        setTrainerPairs(pairs);
        setSelectedInstanceId((prev) => {
          if (prev && cached.some((it) => it.instance_id === prev)) return prev;
          return cached[0].instance_id;
        });
        setTrainerError("Live instance list unavailable; showing cached bots.");
      } else {
        setTrainerError("Could not load model/bot instances.");
      }
    } finally {
      setInstancesLoading(false);
    }
  }

  async function loadExchangeCatalog() {
    try {
      const response = await fetch(`${API_BASE}/terminal/source-coverage?tab_id=default`);
      const payload = (await response.json()) as { exchanges?: Array<{ exchange_id: string; name: string }> };
      const options = Array.isArray(payload.exchanges)
        ? payload.exchanges.map((item) => ({
            exchange_id: String(item.exchange_id).toLowerCase(),
            name: String(item.name),
          }))
        : [];
      setExchangeOptions(options);
    } catch {
      // keep empty catalog and allow selected exchange fallback
    }
  }

  async function loadPairWorkspace(pair: string, instanceId?: string) {
    setTrainerError("");
    if (instanceId) {
      await loadBotSessions(instanceId);
    } else {
      setBotSessions([]);
      setActiveBotSessionId("");
    }
    await Promise.all([
      loadProfile(pair, instanceId),
      loadDataStatus(pair, instanceId),
      loadRuns(pair, instanceId),
      loadBotStatus(pair, instanceId),
    ]);
    void loadBotTrades(pair, instanceId, activeBotSessionId || undefined, "selected");
    void loadBackfillForPair(pair, instanceId);
    if (instanceId) {
      void loadBotTradeMapCandles(instanceId);
    }
  }

  async function refreshLight(pair: string, instanceId?: string) {
    const tasks: Promise<unknown>[] = [
      loadDataStatus(pair, instanceId),
      loadRuns(pair, instanceId),
      loadBotStatus(pair, instanceId),
      backfillJobId ? loadBackfillStatus(backfillJobId) : Promise.resolve(),
    ];
    if (!isDocumentHidden) {
      tasks.push(loadBotTrades(pair, instanceId, activeBotSessionId || undefined, "selected", 150));
    }
    await Promise.all(tasks);
  }

  async function loadProfile(pair: string, instanceId?: string) {
    try {
      const url = instanceId
        ? `${API_BASE}/ml/instances/${encodeURIComponent(instanceId)}/profile`
        : `${API_BASE}/ml/pairs/${encodeURIComponent(pair)}/profile`;
      const response = await fetch(url);
      const payload = (await response.json()) as Partial<MlProfile>;
      const merged = mergeProfile(pair, payload);
      setProfileDraft(merged);
      setProfileSavedSnapshot(JSON.stringify(merged));
    } catch {
      const fallback = mergeProfile(pair, null);
      setProfileDraft(fallback);
      setProfileSavedSnapshot(JSON.stringify(fallback));
      setTrainerError("Could not load profile. Using defaults.");
    }
  }

  async function loadDataStatus(pair: string, instanceId?: string, datasetSourceOverride?: TrainingDatasetSource) {
    try {
      const source = datasetSourceOverride ?? trainingDatasetSource;
      const query = new URLSearchParams({ dataset_source: source });
      const url = instanceId
        ? `${API_BASE}/ml/instances/${encodeURIComponent(instanceId)}/data-status?${query.toString()}`
        : `${API_BASE}/ml/pairs/${encodeURIComponent(pair)}/data-status?${query.toString()}`;
      const response = await fetch(url);
      const payload = (await response.json()) as DataStatus;
      setDataStatus(payload);
    } catch {
      setDataStatus(null);
    }
  }

  async function loadRuns(pair: string, instanceId?: string, preferredRunId?: string) {
    try {
      const query = new URLSearchParams(instanceId ? { instance_id: instanceId } : { pair_symbol: pair });
      const response = await fetch(`${API_BASE}/ml/runs?${query.toString()}`, { cache: "no-store" });
      const payload = (await response.json()) as { runs?: MlRun[] };
      const nextRuns = Array.isArray(payload.runs) ? payload.runs : [];
      setRuns(nextRuns);
      setSelectedRunId((prev) => {
        if (preferredRunId && nextRuns.some((item) => item.run_id === preferredRunId)) {
          return preferredRunId;
        }
        if (prev && nextRuns.some((item) => item.run_id === prev)) return prev;
        return nextRuns[0]?.run_id ?? "";
      });
    } catch {
      setRuns([]);
      setSelectedRunId("");
    }
  }

  async function loadRunDetails(runId: string) {
    try {
      const response = await fetch(`${API_BASE}/ml/runs/${encodeURIComponent(runId)}`, { cache: "no-store" });
      const payload = (await response.json()) as MlRun;
      setRunDetails(payload);
    } catch {
      setRunDetails(null);
    }
  }

  async function loadRunLogs(runId: string) {
    if (logsClearedRunIdRef.current && logsClearedRunIdRef.current === runId) {
      return;
    }
    try {
      const response = await fetch(`${API_BASE}/ml/runs/${encodeURIComponent(runId)}/logs`, { cache: "no-store" });
      const payload = (await response.json()) as { logs?: MlRunLog[] };
      setRunLogs(Array.isArray(payload.logs) ? payload.logs : []);
    } catch {
      setRunLogs([]);
    }
  }

  async function loadBotStatus(pair: string, instanceId?: string) {
    try {
      const url = instanceId
        ? `${API_BASE}/ml/instances/${encodeURIComponent(instanceId)}/bot/status`
        : `${API_BASE}/ml/bot/${encodeURIComponent(pair)}/status`;
      const response = await fetch(url);
      const payload = (await response.json()) as MlBotStatus;
      setBotStatus(payload);
      if (payload.active_session_id) {
        setActiveBotSessionId(payload.active_session_id);
      }
    } catch {
      setBotStatus(null);
    }
  }

  async function loadBotSessions(instanceId?: string) {
    if (!instanceId) {
      setBotSessions([]);
      return;
    }
    try {
      const response = await fetch(`${API_BASE}/ml/instances/${encodeURIComponent(instanceId)}/bot/sessions`);
      const payload = (await response.json()) as { sessions?: MlBotTradeSession[]; active_session_id?: string };
      const sessions = Array.isArray(payload.sessions) ? payload.sessions : [];
      setBotSessions(sessions);
      if (payload.active_session_id) {
        setActiveBotSessionId(payload.active_session_id);
      } else if (sessions.length) {
        setActiveBotSessionId((prev) => prev || sessions[0].session_id);
      }
    } catch {
      setBotSessions([]);
    }
  }

  async function loadBotTrades(
    pair: string,
    instanceId?: string,
    sessionId?: string,
    sessionScope: "selected" | "all" = "selected",
    limit: number = 200,
  ) {
    try {
      const safeLimit = Number.isFinite(limit) ? Math.max(50, Math.min(1000, Math.round(limit))) : 200;
      const params = new URLSearchParams({ limit: String(safeLimit), session_scope: sessionScope });
      if (sessionId) {
        params.set("session_id", sessionId);
      }
      const url = instanceId
        ? `${API_BASE}/ml/instances/${encodeURIComponent(instanceId)}/bot/trades?${params.toString()}`
        : `${API_BASE}/ml/bot/${encodeURIComponent(pair)}/trades?${params.toString()}`;
      const response = await fetch(url);
      const payload = (await response.json()) as {
        trades?: MlBotTrade[];
        active_session_id?: string;
        session_id?: string;
      };
      setBotTrades(Array.isArray(payload.trades) ? payload.trades : []);
      const nextTrades = Array.isArray(payload.trades) ? payload.trades : [];
      if (instanceId) {
        botMapTradeFingerprintRef.current = nextTrades.length
          ? `${nextTrades[0].trade_id}:${nextTrades[0].closed_at}:${nextTrades.length}`
          : "empty";
      }
      if (payload.active_session_id) {
        setActiveBotSessionId(payload.active_session_id);
      } else if (payload.session_id && sessionScope === "selected") {
        setActiveBotSessionId(payload.session_id);
      }
    } catch {
      setBotTrades([]);
    }
  }

  async function loadBotTradeMapCandles(instanceId: string, silent = false) {
    if (!instanceId) {
      setBotMapCandles([]);
      setBotMapInferencePoints([]);
      setBotMapError("");
      return;
    }
    if (!silent) {
      setBotMapLoading(true);
    }
    try {
      const response = await fetch(
        `${API_BASE}/ml/instances/${encodeURIComponent(instanceId)}/bot/trade-map-candles?limit=120`,
        { cache: "no-store" },
      );
      const payload = (await response.json()) as BotTradeMapCandlesResponse;
      if (!response.ok) {
        throw new Error((payload as { detail?: string }).detail || "Could not load trade map candles.");
      }
      setBotMapCandles(Array.isArray(payload.candles) ? payload.candles : []);
      setBotMapInferencePoints(Array.isArray(payload.inference_points) ? payload.inference_points : []);
      setBotMapError("");
    } catch (err) {
      setBotMapCandles([]);
      setBotMapInferencePoints([]);
      setBotMapError(err instanceof Error ? err.message : "Could not load trade map candles.");
    } finally {
      if (!silent) {
        setBotMapLoading(false);
      }
    }
  }

  async function loadBackfillForPair(pair: string, instanceId?: string) {
    try {
      const url = instanceId
        ? `${API_BASE}/ml/instances/${encodeURIComponent(instanceId)}/backfill`
        : `${API_BASE}/ml/pairs/${encodeURIComponent(pair)}/backfill`;
      const response = await fetch(url);
      const payload = (await response.json()) as { job?: BackfillJobStatus | null };
      const job = payload?.job ?? null;
      setBackfillStatus(job);
      setBackfillJobId(job?.job_id ?? "");
      if (job?.job_id) {
        await loadBackfillLogs(job.job_id);
      } else {
        setBackfillLogs([]);
      }
    } catch {
      setBackfillStatus(null);
      setBackfillJobId("");
      setBackfillLogs([]);
    }
  }

  async function setActiveBotSession(sessionId: string) {
    if (!selectedInstanceId || !sessionId) return;
    try {
      const response = await fetch(
        `${API_BASE}/ml/instances/${encodeURIComponent(selectedInstanceId)}/bot/sessions/${encodeURIComponent(sessionId)}`,
        {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ set_active: true }),
        },
      );
      const payload = (await response.json()) as { active_session_id?: string; detail?: string };
      if (!response.ok) {
        throw new Error(payload.detail || "Failed to select session.");
      }
      await loadBotSessions(selectedInstanceId);
      setActiveBotSessionId(payload.active_session_id || sessionId);
      await loadBotTrades(selectedPair, selectedInstanceId, payload.active_session_id || sessionId, "selected");
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Failed to select session.");
    }
  }

  async function createBotSession() {
    if (!selectedInstanceId) return;
    const name = botSessionInput.trim();
    if (!name) {
      setTrainerError("Enter a session name.");
      return;
    }
    try {
      const response = await fetch(`${API_BASE}/ml/instances/${encodeURIComponent(selectedInstanceId)}/bot/sessions`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      });
      const payload = (await response.json()) as { detail?: string };
      if (!response.ok) {
        throw new Error(payload.detail || "Failed to create session.");
      }
      setBotSessionInput("");
      setBotSessionCreateOpen(false);
      await loadBotSessions(selectedInstanceId);
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Failed to create session.");
    }
  }

  async function renameBotSession() {
    if (!selectedInstanceId || !activeBotSessionId || !selectedBotSession) return;
    const name = botSessionInput.trim();
    if (!name) {
      setTrainerError("Enter a session name.");
      return;
    }
    try {
      const response = await fetch(
        `${API_BASE}/ml/instances/${encodeURIComponent(selectedInstanceId)}/bot/sessions/${encodeURIComponent(activeBotSessionId)}`,
        {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name }),
        },
      );
      const payload = (await response.json()) as { detail?: string };
      if (!response.ok) {
        throw new Error(payload.detail || "Failed to rename session.");
      }
      setBotSessionInput("");
      setBotSessionRenameOpen(false);
      await loadBotSessions(selectedInstanceId);
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Failed to rename session.");
    }
  }

  async function deleteBotSession() {
    if (!selectedInstanceId || !activeBotSessionId || !selectedBotSession) return;
    if (selectedBotSession.is_default) {
      setTrainerError("Default session cannot be deleted.");
      return;
    }
    if (!window.confirm(`Delete session "${selectedBotSession.name}"? Trades will move to Default.`)) {
      return;
    }
    try {
      const response = await fetch(
        `${API_BASE}/ml/instances/${encodeURIComponent(selectedInstanceId)}/bot/sessions/${encodeURIComponent(activeBotSessionId)}`,
        { method: "DELETE" },
      );
      const payload = (await response.json()) as { active_session_id?: string; detail?: string };
      if (!response.ok) {
        throw new Error(payload.detail || "Failed to delete session.");
      }
      await loadBotSessions(selectedInstanceId);
      if (payload.active_session_id) {
        setActiveBotSessionId(payload.active_session_id);
      }
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Failed to delete session.");
    }
  }

  async function clearBotSessionHistory() {
    if (!selectedInstanceId || !activeBotSessionId || !selectedBotSession) return;
    if (!window.confirm(`Clear all trade history in "${selectedBotSession.name}" session?`)) {
      return;
    }
    try {
      const response = await fetch(
        `${API_BASE}/ml/instances/${encodeURIComponent(selectedInstanceId)}/bot/sessions/${encodeURIComponent(activeBotSessionId)}/clear`,
        { method: "POST" },
      );
      const payload = (await response.json()) as { detail?: string; deleted_trades?: number };
      if (!response.ok) {
        throw new Error(payload.detail || "Failed to clear session history.");
      }
      await loadBotSessions(selectedInstanceId);
      await loadBotTrades(selectedPair, selectedInstanceId, activeBotSessionId, "selected");
      setTrainerStatus(`Cleared ${Number(payload.deleted_trades || 0)} trades from session.`);
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Failed to clear session history.");
    }
  }

  async function loadBackfillStatus(jobId: string) {
    try {
      const response = await fetch(`${API_BASE}/ml/backfill/${encodeURIComponent(jobId)}`);
      const payload = (await response.json()) as BackfillJobStatus;
      setBackfillStatus(payload);
    } catch {
      // keep last status snapshot on transient errors
    }
  }

  async function loadBackfillLogs(jobId: string) {
    try {
      const response = await fetch(`${API_BASE}/ml/backfill/${encodeURIComponent(jobId)}/logs`);
      const payload = (await response.json()) as { logs?: MlRunLog[] };
      setBackfillLogs(Array.isArray(payload.logs) ? payload.logs : []);
    } catch {
      // keep last logs
    }
  }

  async function saveProfile(): Promise<boolean> {
    if (!selectedPair || !profileDraft) return false;
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const url = selectedInstanceId
        ? `${API_BASE}/ml/instances/${encodeURIComponent(selectedInstanceId)}/profile`
        : `${API_BASE}/ml/pairs/${encodeURIComponent(selectedPair)}/profile`;
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...profileDraft,
          selected_exchanges: profileDraft.training_selected_exchanges,
        }),
      });
      if (!response.ok) {
        const body = (await response.json().catch(() => ({}))) as { detail?: string };
        throw new Error(body.detail || "Profile save failed.");
      }
      const payload = (await response.json()) as Partial<MlProfile>;
      const merged = mergeProfile(selectedPair, payload);
      setProfileDraft(merged);
      setProfileSavedSnapshot(JSON.stringify(merged));
      setTrainerStatus(`Saved profile for ${selectedPair}.`);
      await loadInstances();
      return true;
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Profile save failed.");
      return false;
    } finally {
      setTrainerBusy(false);
    }
  }

  async function addPair() {
    const pair = normalizePair(pairInput);
    if (!pair || !pair.endsWith("USDT")) {
      setTrainerError("Use BASEUSDT format (example: BTCUSDT).");
      return;
    }
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const response = await fetch(`${API_BASE}/ml/instances`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ pair_symbol: pair, name: `${pair} Bot` }),
      });
      if (!response.ok) {
        const body = (await response.json().catch(() => ({}))) as { detail?: string };
        throw new Error(body.detail || "Could not add pair.");
      }
      const payload = (await response.json()) as MlInstance & { detail?: string };
      if (!payload.instance_id) {
        throw new Error(payload.detail || "Could not create model/bot.");
      }
      await loadInstances();
      setSelectedInstanceId(payload.instance_id);
      setSelectedPair(pair);
      setPairInput("");
      setTrainerStatus(`Created model/bot for ${pair}.`);
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Could not add pair.");
    } finally {
      setTrainerBusy(false);
    }
  }

  async function createInstanceFromPrompt() {
    const pairRaw = window.prompt("Pair symbol (BASEUSDT), e.g. BTCUSDT", selectedPair || "BTCUSDT");
    if (!pairRaw) return;
    const pair = normalizePair(pairRaw);
    if (!pair || !pair.endsWith("USDT")) {
      setTrainerError("Use BASEUSDT format (example: BTCUSDT).");
      return;
    }
    const nameRaw = window.prompt("Model/Bot name", `${pair} Bot`);
    if (!nameRaw || !nameRaw.trim()) return;
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const response = await fetch(`${API_BASE}/ml/instances`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ pair_symbol: pair, name: nameRaw.trim() }),
      });
      const payload = (await response.json().catch(() => ({}))) as MlInstance & { detail?: string };
      if (!response.ok || !payload.instance_id) {
        throw new Error(payload.detail || "Could not create model/bot.");
      }
      await loadInstances();
      setSelectedInstanceId(payload.instance_id);
      setSelectedPair(pair);
      setMlDetailsOpen(true);
      setTrainerStatus(`Created ${nameRaw.trim()} for ${pair}.`);
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Could not create model/bot.");
    } finally {
      setTrainerBusy(false);
    }
  }

  async function renameSelectedInstance() {
    if (!selectedInstanceId) return;
    const current = instances.find((it) => it.instance_id === selectedInstanceId);
    const nextName = window.prompt("Enter new model/bot name", current?.name || "");
    if (!nextName || !nextName.trim()) return;
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const response = await fetch(`${API_BASE}/ml/instances/${encodeURIComponent(selectedInstanceId)}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: nextName.trim() }),
      });
      const payload = (await response.json().catch(() => ({}))) as { detail?: string };
      if (!response.ok) {
        throw new Error(payload.detail || "Rename failed.");
      }
      await loadInstances();
      setTrainerStatus("Model/bot renamed.");
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Rename failed.");
    } finally {
      setTrainerBusy(false);
    }
  }

  async function deleteInstance(instanceId: string, instanceName: string) {
    const confirmed = window.confirm(`Delete bot/model "${instanceName}"?`);
    if (!confirmed) return;
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const response = await fetch(`${API_BASE}/ml/instances/${encodeURIComponent(instanceId)}`, {
        method: "DELETE",
      });
      const payload = (await response.json().catch(() => ({}))) as { detail?: string };
      if (!response.ok) {
        throw new Error(payload.detail || "Delete failed.");
      }
      if (selectedInstanceId === instanceId) {
        setSelectedInstanceId("");
        setSelectedPair("");
        setSelectedRunId("");
        setRunDetails(null);
        setRunLogs([]);
        setMlDetailsOpen(false);
      }
      await loadInstances();
      setTrainerStatus(`Deleted ${instanceName}.`);
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Delete failed.");
    } finally {
      setTrainerBusy(false);
    }
  }

  function beginRunActionProgress(action: RunActionName) {
    setRunActionProgress({
      action,
      startedAtMs: Date.now(),
      etaMs: RUN_ACTION_ETA_MS[action],
      state: "active",
    });
  }

  function finishRunActionProgress() {
    setRunActionProgress((prev) => {
      if (!prev) return prev;
      return { ...prev, state: "done" };
    });
    window.setTimeout(() => {
      setRunActionProgress((prev) => (prev?.state === "done" ? null : prev));
    }, 900);
  }

  async function actionStartRun() {
    if (workloadControls && !workloadControls.training_enabled) {
      setTrainerError("Training Jobs are disabled in Workload Controls.");
      return;
    }
    if (!selectedPair) return;
    beginRunActionProgress("start");
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const response = await fetch(`${API_BASE}/ml/runs/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(
          selectedInstanceId
            ? { instance_id: selectedInstanceId, dataset_source: trainingDatasetSource }
            : { pair_symbol: selectedPair, dataset_source: trainingDatasetSource }
        ),
      });
      const payload = (await response.json()) as { ok?: boolean; run_id?: string; detail?: string; resumed?: boolean };
      if (!response.ok || !payload.ok || !payload.run_id) {
        throw new Error(payload.detail || "Run start failed.");
      }
      setSelectedRunId(payload.run_id);
      // Release UI immediately after run is queued; refresh details in background.
      finishRunActionProgress();
      setTrainerBusy(false);
      void loadRuns(selectedPair, selectedInstanceId || undefined, payload.run_id);
      void loadRunDetails(payload.run_id);
      void loadRunLogs(payload.run_id);
      setTrainerStatus(payload.resumed ? "Run resumed." : "Run started.");
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Run start failed.");
      finishRunActionProgress();
      setTrainerBusy(false);
    }
  }

  async function actionPauseRun() {
    if (!selectedRunId) return;
    beginRunActionProgress("pause");
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const response = await fetch(`${API_BASE}/ml/runs/pause`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ run_id: selectedRunId }),
      });
      const payload = (await response.json()) as { ok?: boolean; detail?: string };
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || "Pause failed.");
      }
      if (selectedPair) {
        await loadRuns(selectedPair, selectedInstanceId || undefined);
        await loadRunDetails(selectedRunId);
      }
      setTrainerStatus("Pause requested.");
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Pause failed.");
    } finally {
      finishRunActionProgress();
      setTrainerBusy(false);
    }
  }

  async function actionStopRun() {
    if (!selectedRunId) return;
    const targetRunId = selectedRunId;
    beginRunActionProgress("stop");
    setStopRequestedRunId(targetRunId);
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const response = await fetch(`${API_BASE}/ml/runs/stop`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ run_id: selectedRunId }),
      });
      const payload = (await response.json()) as { ok?: boolean; detail?: string };
      if (response.status === 503) {
        setAutoStopSuppressed(true);
        throw new Error(payload.detail || "Stop unavailable while Training Jobs service is disabled.");
      }
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || "Stop failed.");
      }
      // Unlock immediately and refresh status in background.
      finishRunActionProgress();
      setTrainerBusy(false);
      if (selectedPair) {
        void loadRuns(selectedPair, selectedInstanceId || undefined);
        void loadRunDetails(targetRunId);
        void loadRunLogs(targetRunId);
      }
      setTrainerStatus("Stop requested.");
    } catch (err) {
      setStopRequestedRunId("");
      setTrainerError(err instanceof Error ? err.message : "Stop failed.");
      finishRunActionProgress();
      setTrainerBusy(false);
    }
  }

  async function actionDeleteRun(runId: string) {
    if (!runId) return;
    const confirmed = window.confirm(`Delete run ${runId}? This cannot be undone.`);
    if (!confirmed) return;
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const response = await fetch(`${API_BASE}/ml/runs/${encodeURIComponent(runId)}`, {
        method: "DELETE",
      });
      const payload = (await response.json().catch(() => ({}))) as { ok?: boolean; detail?: string };
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || "Delete failed.");
      }
      const wasSelected = selectedRunId === runId;
      if (selectedPair) {
        await loadRuns(selectedPair, selectedInstanceId || undefined);
      }
      if (wasSelected) {
        setRunDetails(null);
        setRunLogs([]);
      }
      setTrainerStatus(`Deleted run ${runId}.`);
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Delete failed.");
    } finally {
      setTrainerBusy(false);
    }
  }

  function formatRunLabel(run: MlRun): string {
    const ts = run.created_at ? new Date(run.created_at).toLocaleString() : "-";
    return `${run.pair_symbol} | ${ts} | ${run.status}`;
  }

  async function actionApproveRun() {
    if (!selectedRunId) return;
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const response = await fetch(`${API_BASE}/ml/runs/${encodeURIComponent(selectedRunId)}/approve`, {
        method: "POST",
      });
      const payload = (await response.json()) as { ok?: boolean; detail?: string };
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || "Approve failed.");
      }
      setTrainerStatus("Run approved and promoted to current model.");
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Approve failed.");
    } finally {
      setTrainerBusy(false);
    }
  }

  async function actionApplyRecommendedGates() {
    if (!selectedRunId || !selectedPair) return;
    setTrainerError("");
    setTrainerStatus("Applying recommended gates to pair profile...");
    try {
      const response = await fetch(`${API_BASE}/ml/runs/${encodeURIComponent(selectedRunId)}/apply-recommended-gates`, {
        method: "POST",
      });
      const payload = (await response.json()) as Record<string, unknown>;
      if (!response.ok) {
        throw new Error(String(payload?.detail || `Failed to apply recommended gates (${response.status})`));
      }
      await loadProfile(selectedPair);
      setTrainerStatus("Recommended gate thresholds applied to pair profile.");
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Failed to apply recommended gates.");
    }
  }

  async function actionBotStart() {
    if (workloadControls && !workloadControls.bots_enabled) {
      setTrainerError("Bot Engine is disabled in Workload Controls.");
      return;
    }
    if (!selectedPair) return;
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const response = await fetch(
        selectedInstanceId
          ? `${API_BASE}/ml/instances/${encodeURIComponent(selectedInstanceId)}/bot/start`
          : `${API_BASE}/ml/bot/${encodeURIComponent(selectedPair)}/start`,
        { method: "POST" },
      );
      const payload = (await response.json()) as { ok?: boolean; detail?: string };
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || "Bot start failed.");
      }
      await loadBotStatus(selectedPair, selectedInstanceId || undefined);
      setTrainerStatus("Paper bot started.");
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Bot start failed.");
    } finally {
      setTrainerBusy(false);
    }
  }

  async function actionBotStop() {
    if (!selectedPair) return;
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const response = await fetch(
        selectedInstanceId
          ? `${API_BASE}/ml/instances/${encodeURIComponent(selectedInstanceId)}/bot/stop`
          : `${API_BASE}/ml/bot/${encodeURIComponent(selectedPair)}/stop`,
        { method: "POST" },
      );
      const payload = (await response.json()) as { ok?: boolean; detail?: string };
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || "Bot stop failed.");
      }
      await loadBotStatus(selectedPair, selectedInstanceId || undefined);
      setTrainerStatus("Paper bot stopped.");
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Bot stop failed.");
    } finally {
      setTrainerBusy(false);
    }
  }

  async function actionBotPause() {
    if (!selectedPair) return;
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const response = await fetch(
        selectedInstanceId
          ? `${API_BASE}/ml/instances/${encodeURIComponent(selectedInstanceId)}/bot/pause`
          : `${API_BASE}/ml/bot/${encodeURIComponent(selectedPair)}/pause`,
        { method: "POST" },
      );
      const payload = (await response.json()) as { ok?: boolean; detail?: string };
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || "Bot pause failed.");
      }
      await loadBotStatus(selectedPair, selectedInstanceId || undefined);
      setTrainerStatus("Paper bot paused.");
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Bot pause failed.");
    } finally {
      setTrainerBusy(false);
    }
  }

  async function copyRunLogsToClipboard() {
    if (!runLogs.length) {
      setTrainerError("No run logs to copy.");
      return;
    }
    const text = runLogs
      .map((log) => `${new Date(log.ts).toLocaleTimeString()} [${log.level}] ${log.message}`)
      .join("\n");
    try {
      await navigator.clipboard.writeText(text);
      setTrainerError("");
      setTrainerStatus(`Copied ${runLogs.length} log lines.`);
    } catch {
      setTrainerStatus("");
      setTrainerError("Could not copy logs to clipboard.");
    }
  }

  function clearRunLogsPanel() {
    setRunLogs([]);
    if (selectedRunId) {
      setLogsClearedRunId(selectedRunId);
      logsClearedRunIdRef.current = selectedRunId;
    }
    setTrainerError("");
    setTrainerStatus("Run logs cleared from view.");
  }

  function downloadTextFile(filename: string, content: string, mime = "text/plain;charset=utf-8") {
    const blob = new Blob([content], { type: mime });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
  }

  function escapePdfText(value: string): string {
    return value.replace(/\\/g, "\\\\").replace(/\(/g, "\\(").replace(/\)/g, "\\)");
  }

  function buildSimplePdf(textLines: string[]): string {
    const contentLines = ["BT", "/F1 9 Tf", "36 806 Td", "11 TL"];
    for (const line of textLines) {
      contentLines.push(`(${escapePdfText(line)}) Tj`);
      contentLines.push("T*");
    }
    contentLines.push("ET");
    const stream = contentLines.join("\n");
    const objects = [
      "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
      "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
      "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>\nendobj\n",
      "4 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n",
      `5 0 obj\n<< /Length ${stream.length} >>\nstream\n${stream}\nendstream\nendobj\n`,
    ];
    let pdf = "%PDF-1.4\n";
    const offsets = [0];
    for (const obj of objects) {
      offsets.push(pdf.length);
      pdf += obj;
    }
    const xrefStart = pdf.length;
    pdf += `xref\n0 ${objects.length + 1}\n`;
    pdf += "0000000000 65535 f \n";
    for (let i = 1; i < offsets.length; i += 1) {
      pdf += `${String(offsets[i]).padStart(10, "0")} 00000 n \n`;
    }
    pdf += `trailer\n<< /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xrefStart}\n%%EOF`;
    return pdf;
  }

  async function exportBotTrades(format: "text" | "doc" | "pdf" | "csv", scope: "selected" | "all") {
    if (!selectedPair) return;
    const exportTrades =
      scope === "selected"
        ? botTrades
        : await (async () => {
            try {
              const params = new URLSearchParams({ limit: "5000", session_scope: "all" });
              const url = selectedInstanceId
                ? `${API_BASE}/ml/instances/${encodeURIComponent(selectedInstanceId)}/bot/trades?${params.toString()}`
                : `${API_BASE}/ml/bot/${encodeURIComponent(selectedPair)}/trades?${params.toString()}`;
              const response = await fetch(url);
              const payload = (await response.json()) as { trades?: MlBotTrade[] };
              return Array.isArray(payload.trades) ? payload.trades : [];
            } catch {
              return [];
            }
          })();
    if (!exportTrades.length) {
      setTrainerError("No bot trades to export.");
      return;
    }
    const sortedByTime = [...exportTrades].sort(
      (a, b) => new Date(a.closed_at).getTime() - new Date(b.closed_at).getTime()
    );
    const wins = sortedByTime.filter((t) => Number(t.net_pnl) > 0);
    const losses = sortedByTime.filter((t) => Number(t.net_pnl) < 0);
    const grossProfit = wins.reduce((sum, t) => sum + Number(t.net_pnl || 0), 0);
    const grossLossAbs = Math.abs(losses.reduce((sum, t) => sum + Number(t.net_pnl || 0), 0));
    const totalNet = sortedByTime.reduce((sum, t) => sum + Number(t.net_pnl || 0), 0);
    const initialBalance = Number(profileDraft?.paper_bot?.initial_balance ?? botStatus?.initial_balance ?? 100);
    const endingBalance = Number(botStatus?.current_balance ?? initialBalance + Number(botStatus?.realized_pnl ?? totalNet));
    const endingEquity = Number(botStatus?.equity ?? endingBalance + Number(botStatus?.unrealized_pnl ?? 0));
    const winRate = sortedByTime.length ? (wins.length / sortedByTime.length) * 100 : 0;
    const avgWin = wins.length ? grossProfit / wins.length : 0;
    const avgLoss = losses.length ? losses.reduce((sum, t) => sum + Number(t.net_pnl || 0), 0) / losses.length : 0;
    const profitFactor = grossLossAbs > 0 ? grossProfit / grossLossAbs : (grossProfit > 0 ? Number.POSITIVE_INFINITY : 0);
    let equity = 0;
    let peak = 0;
    let maxDrawdownProxy = 0;
    for (const t of sortedByTime) {
      equity += Number(t.net_pnl || 0);
      peak = Math.max(peak, equity);
      maxDrawdownProxy = Math.max(maxDrawdownProxy, peak - equity);
    }
    const bestTrade = [...sortedByTime].sort((a, b) => Number(b.net_pnl || 0) - Number(a.net_pnl || 0))[0] || null;
    const worstTrade = [...sortedByTime].sort((a, b) => Number(a.net_pnl || 0) - Number(b.net_pnl || 0))[0] || null;
    const reasonCounts = new Map<string, number>();
    for (const t of sortedByTime) {
      const key = String(t.close_reason || "unknown");
      reasonCounts.set(key, (reasonCounts.get(key) || 0) + 1);
    }
    const reasonBreakdown = [...reasonCounts.entries()]
      .sort((a, b) => b[1] - a[1])
      .map(([reason, count]) => `${reason}: ${count}`)
      .join(", ");
    setBotExportMenuOpen(false);
    const scopeLabel = scope === "all" ? "all_sessions" : (selectedBotSession?.name || "selected_session").replace(/\s+/g, "_");
    const baseName = `bot_trade_report_${selectedPair || "PAIR"}_${scopeLabel}_${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-")}`;
    const sessionsById = new Map(botSessions.map((s) => [s.session_id, s.name]));
    const textLines = [
      `BOT TRADE REPORT - ${selectedPair || "-"}`,
      `Generated: ${new Date().toLocaleString()}`,
      `Scope: ${scope === "all" ? "All Sessions" : `Session: ${selectedBotSession?.name || "-"}`}`,
      `Trades: ${exportTrades.length}`,
      "",
      "PROFESSIONAL SUMMARY",
      `Initial balance: ${initialBalance.toFixed(6)}`,
      `Ending balance: ${endingBalance.toFixed(6)}`,
      `Ending equity: ${endingEquity.toFixed(6)}`,
      `Total net PnL: ${totalNet.toFixed(6)}`,
      `Win rate: ${winRate.toFixed(2)}%`,
      `Profit factor: ${Number.isFinite(profitFactor) ? profitFactor.toFixed(4) : "INF"}`,
      `Avg win: ${avgWin.toFixed(6)}`,
      `Avg loss: ${avgLoss.toFixed(6)}`,
      `Max drawdown (proxy): ${maxDrawdownProxy.toFixed(6)}`,
      `Best trade: ${bestTrade ? `${bestTrade.side} net=${Number(bestTrade.net_pnl || 0).toFixed(6)} reason=${bestTrade.close_reason}` : "-"}`,
      `Worst trade: ${worstTrade ? `${worstTrade.side} net=${Number(worstTrade.net_pnl || 0).toFixed(6)} reason=${worstTrade.close_reason}` : "-"}`,
      `Reason breakdown: ${reasonBreakdown || "-"}`,
      "",
    ];
    if (scope === "all") {
      const bySession = new Map<string, MlBotTrade[]>();
      for (const trade of exportTrades) {
        const key = sessionsById.get(String(trade.session_id || "")) || "Unknown";
        if (!bySession.has(key)) bySession.set(key, []);
        bySession.get(key)?.push(trade);
      }
      let section = 1;
      for (const [sessionName, trades] of bySession.entries()) {
        textLines.push(`SESSION ${section}: ${sessionName} (${trades.length} trades)`);
        trades.forEach((t, i) => {
          textLines.push(
            `  ${i + 1}. ${t.side} qty=${t.qty} lev=${t.leverage}x entry=${t.entry_price} exit=${t.exit_price} net_pnl=${t.net_pnl} opened=${new Date(t.opened_at).toLocaleString()} closed=${new Date(t.closed_at).toLocaleString()} reason=${t.close_reason}`
          );
        });
        textLines.push("");
        section += 1;
      }
    } else {
      textLines.push(
        ...exportTrades.map(
          (t, i) =>
            `${i + 1}. ${t.side} qty=${t.qty} lev=${t.leverage}x entry=${t.entry_price} exit=${t.exit_price} net_pnl=${t.net_pnl} opened=${new Date(t.opened_at).toLocaleString()} closed=${new Date(t.closed_at).toLocaleString()} reason=${t.close_reason}`
        )
      );
    }
    if (format === "text") {
      downloadTextFile(`${baseName}.txt`, textLines.join("\n"));
      return;
    }
    if (format === "pdf") {
      const pdfText = buildSimplePdf(textLines);
      const blob = new Blob([pdfText], { type: "application/pdf;charset=utf-8" });
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = `${baseName}.pdf`;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(url);
      return;
    }
    if (format === "csv") {
      const header = [
        "trade_id",
        "session_name",
        "pair_symbol",
        "side",
        "qty",
        "leverage",
        "commission_fee_pct",
        "entry_price",
        "exit_price",
        "entry_notional",
        "exit_notional",
        "entry_fee",
        "exit_fee",
        "gross_pnl",
        "net_pnl",
        "opened_at",
        "closed_at",
        "duration_seconds",
        "close_reason",
      ];
      const esc = (v: unknown) => `"${String(v ?? "").replace(/"/g, '""')}"`;
      const summaryRows = [
        ["SUMMARY", "Initial balance", initialBalance.toFixed(6)],
        ["SUMMARY", "Ending balance", endingBalance.toFixed(6)],
        ["SUMMARY", "Ending equity", endingEquity.toFixed(6)],
        ["SUMMARY", "Total net PnL", totalNet.toFixed(6)],
        ["SUMMARY", "Win rate", `${winRate.toFixed(2)}%`],
        ["SUMMARY", "Profit factor", Number.isFinite(profitFactor) ? profitFactor.toFixed(4) : "INF"],
        ["SUMMARY", "Avg win", avgWin.toFixed(6)],
        ["SUMMARY", "Avg loss", avgLoss.toFixed(6)],
        ["SUMMARY", "Max drawdown (proxy)", maxDrawdownProxy.toFixed(6)],
        ["SUMMARY", "Best trade", bestTrade ? `${bestTrade.trade_id} ${bestTrade.side} ${Number(bestTrade.net_pnl || 0).toFixed(6)}` : "-"],
        ["SUMMARY", "Worst trade", worstTrade ? `${worstTrade.trade_id} ${worstTrade.side} ${Number(worstTrade.net_pnl || 0).toFixed(6)}` : "-"],
        ["SUMMARY", "Reason breakdown", reasonBreakdown || "-"],
      ].map((row) => row.map(esc).join(","));
      const rows = exportTrades.map((t) =>
        [
          t.trade_id,
          sessionsById.get(String(t.session_id || "")) || "",
          t.pair_symbol,
          t.side,
          t.qty,
          t.leverage,
          t.commission_fee_pct,
          t.entry_price,
          t.exit_price,
          t.entry_notional,
          t.exit_notional,
          t.entry_fee,
          t.exit_fee,
          t.gross_pnl,
          t.net_pnl,
          t.opened_at,
          t.closed_at,
          t.duration_seconds,
          t.close_reason,
        ]
          .map(esc)
          .join(",")
      );
      downloadTextFile(`${baseName}.csv`, [...summaryRows, "", header.join(","), ...rows].join("\n"), "text/csv;charset=utf-8");
      return;
    }
    if (format === "doc") {
      const bySession = new Map<string, MlBotTrade[]>();
      for (const trade of exportTrades) {
        const key = sessionsById.get(String(trade.session_id || "")) || "Unknown";
        if (!bySession.has(key)) bySession.set(key, []);
        bySession.get(key)?.push(trade);
      }
      const htmlRows =
        scope === "all"
          ? [...bySession.entries()]
              .map(([sessionName, trades]) => {
                const rows = trades
                  .map(
                    (t) =>
                      `<tr><td>${t.trade_id}</td><td>${sessionName}</td><td>${t.pair_symbol}</td><td>${t.side}</td><td>${t.qty}</td><td>${t.leverage}x</td><td>${t.entry_price}</td><td>${t.exit_price}</td><td>${t.net_pnl.toFixed(6)}</td><td>${new Date(t.opened_at).toLocaleString()}</td><td>${new Date(t.closed_at).toLocaleString()}</td><td>${t.close_reason}</td></tr>`
                  )
                  .join("");
                return `<tr><td colspan="12"><b>${sessionName}</b></td></tr>${rows}`;
              })
              .join("")
          : exportTrades
              .map(
                (t) =>
                  `<tr><td>${t.trade_id}</td><td>${sessionsById.get(String(t.session_id || "")) || ""}</td><td>${t.pair_symbol}</td><td>${t.side}</td><td>${t.qty}</td><td>${t.leverage}x</td><td>${t.entry_price}</td><td>${t.exit_price}</td><td>${t.net_pnl.toFixed(6)}</td><td>${new Date(t.opened_at).toLocaleString()}</td><td>${new Date(t.closed_at).toLocaleString()}</td><td>${t.close_reason}</td></tr>`
              )
              .join("");
      const html = `<!doctype html><html><head><meta charset="utf-8"><title>Bot Trade Report</title></head><body><h2>Bot Trade Report - ${
        selectedPair || "-"
      }</h2><p>Generated: ${new Date().toLocaleString()}</p><p>Scope: ${
        scope === "all" ? "All Sessions" : `Session: ${selectedBotSession?.name || "-"}`
      }</p><h3>Professional Summary</h3><ul><li>Total net PnL: ${totalNet.toFixed(6)}</li><li>Win rate: ${winRate.toFixed(2)}%</li><li>Profit factor: ${
        Number.isFinite(profitFactor) ? profitFactor.toFixed(4) : "INF"
      }</li><li>Initial balance: ${initialBalance.toFixed(6)}</li><li>Ending balance: ${endingBalance.toFixed(6)}</li><li>Ending equity: ${endingEquity.toFixed(6)}</li><li>Avg win: ${avgWin.toFixed(6)}</li><li>Avg loss: ${avgLoss.toFixed(6)}</li><li>Max drawdown (proxy): ${maxDrawdownProxy.toFixed(6)}</li><li>Best trade: ${
        bestTrade ? `${bestTrade.trade_id} ${bestTrade.side} ${Number(bestTrade.net_pnl || 0).toFixed(6)}` : "-"
      }</li><li>Worst trade: ${
        worstTrade ? `${worstTrade.trade_id} ${worstTrade.side} ${Number(worstTrade.net_pnl || 0).toFixed(6)}` : "-"
      }</li><li>Reason breakdown: ${reasonBreakdown || "-"}</li></ul><table border="1" cellspacing="0" cellpadding="4"><tr><th>Trade ID</th><th>Session</th><th>Pair</th><th>Side</th><th>Qty</th><th>Lev</th><th>Entry</th><th>Exit</th><th>Net PnL</th><th>Opened</th><th>Closed</th><th>Reason</th></tr>${htmlRows}</table></body></html>`;
      downloadTextFile(`${baseName}.doc`, html, "application/msword;charset=utf-8");
      return;
    }
  }

  async function actionBackfillStart() {
    if (workloadControls && !workloadControls.backfill_enabled) {
      setTrainerError("Backfill Jobs are disabled in Workload Controls.");
      return;
    }
    if (!selectedPair) return;
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const response = await fetch(`${API_BASE}/ml/backfill/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(selectedInstanceId ? { instance_id: selectedInstanceId } : { pair_symbol: selectedPair }),
      });
      const payload = (await response.json()) as { ok?: boolean; job_id?: string; detail?: string; resumed?: boolean };
      if (!response.ok || !payload.ok || !payload.job_id) {
        throw new Error(payload.detail || "Historic data acquire failed.");
      }
      setBackfillJobId(payload.job_id);
      await loadBackfillStatus(payload.job_id);
      await loadBackfillLogs(payload.job_id);
      await loadDataStatus(selectedPair, selectedInstanceId || undefined);
      setTrainerStatus(payload.resumed ? "Historic data acquisition resumed." : "Historic data acquisition started.");
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Historic data acquire failed.");
    } finally {
      setTrainerBusy(false);
    }
  }

  async function actionBackfillPause() {
    if (!backfillJobId) return;
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const response = await fetch(`${API_BASE}/ml/backfill/pause`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ job_id: backfillJobId }),
      });
      const payload = (await response.json()) as { ok?: boolean; detail?: string };
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || "Backfill pause failed.");
      }
      await loadBackfillStatus(backfillJobId);
      setTrainerStatus("Historic data acquisition paused.");
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Backfill pause failed.");
    } finally {
      setTrainerBusy(false);
    }
  }

  async function actionBackfillStop() {
    if (!backfillJobId) return;
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const response = await fetch(`${API_BASE}/ml/backfill/stop`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ job_id: backfillJobId }),
      });
      const payload = (await response.json()) as { ok?: boolean; detail?: string };
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || "Backfill stop failed.");
      }
      await loadBackfillStatus(backfillJobId);
      await loadDataStatus(selectedPair, selectedInstanceId || undefined);
      setTrainerStatus("Historic data acquisition stopped.");
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Backfill stop failed.");
    } finally {
      setTrainerBusy(false);
    }
  }

  async function actionBackfillClear() {
    if (!selectedPair) return;
    const confirmed = window.confirm("Delete all acquired historic data for this instance? This cannot be undone.");
    if (!confirmed) return;
    setTrainerBusy(true);
    setTrainerError("");
    setTrainerStatus("");
    try {
      const response = await fetch(`${API_BASE}/ml/backfill/clear`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          pair_symbol: selectedPair,
          instance_id: selectedInstanceId || null,
        }),
      });
      const payload = (await response.json()) as { ok?: boolean; detail?: string; removed_files?: number; removed_rows?: number };
      if (!response.ok || !payload.ok) {
        throw new Error(payload.detail || "Historic data delete failed.");
      }
      setBackfillJobId("");
      setBackfillStatus(null);
      setBackfillLogs([]);
      await loadDataStatus(selectedPair, selectedInstanceId || undefined);
      setTrainerStatus(
        `Historic data deleted. files=${Number(payload.removed_files ?? 0).toLocaleString()} rows=${Number(payload.removed_rows ?? 0).toLocaleString()}.`
      );
    } catch (err) {
      setTrainerError(err instanceof Error ? err.message : "Historic data delete failed.");
    } finally {
      setTrainerBusy(false);
    }
  }

  function updateProfileField<K extends keyof MlProfile>(key: K, value: MlProfile[K]) {
    setProfileDraft((prev) => (prev ? { ...prev, [key]: value } : prev));
  }

  function updateTrainingField<K extends keyof MlProfile["training"]>(key: K, value: MlProfile["training"][K]) {
    setProfileDraft((prev) => {
      if (!prev) return prev;
      return {
        ...prev,
        training: {
          ...prev.training,
          [key]: value,
        },
      };
    });
  }

  function updatePaperBotField<K extends keyof MlProfile["paper_bot"]>(key: K, value: MlProfile["paper_bot"][K]) {
    setProfileDraft((prev) => {
      if (!prev) return prev;
      return {
        ...prev,
        paper_bot: {
          ...prev.paper_bot,
          [key]: value,
        },
      };
    });
  }

  function toggleExchangeList(
    key: "training_selected_exchanges" | "bot_data_selected_exchanges" | "bot_execution_selected_exchanges",
    exchangeId: string,
    checked: boolean
  ) {
    setProfileDraft((prev) => {
      if (!prev) return prev;
      const current = new Set((prev[key] || []).map((item) => item.toLowerCase()));
      if (checked) current.add(exchangeId.toLowerCase());
      else current.delete(exchangeId.toLowerCase());
      return {
        ...prev,
        [key]: [...current].sort(),
      };
    });
  }

  function setExchangeListAll(
    key: "training_selected_exchanges" | "bot_data_selected_exchanges" | "bot_execution_selected_exchanges",
    checked: boolean
  ) {
    setProfileDraft((prev) => {
      if (!prev) return prev;
      return {
        ...prev,
        [key]: checked ? allExchangeOptions.map((option) => option.exchange_id) : [],
      };
    });
  }

  function openProfileModal() {
    if (!selectedPair) return;
    setProfileModalSection("pair");
    setProfileModalOpen(true);
    if (!profileDraft) {
      void loadProfile(selectedPair, selectedInstanceId || undefined);
    }
  }

  function openBotSettingsModal() {
    if (!selectedPair) return;
    setProfileModalSection("bot");
    setProfileModalOpen(true);
    if (!profileDraft) {
      void loadProfile(selectedPair, selectedInstanceId || undefined);
    }
  }

  function openReplayTraceSettingsModal() {
    if (!selectedPair) return;
    setProfileModalSection("trace");
    setProfileModalOpen(true);
    if (!profileDraft) {
      void loadProfile(selectedPair, selectedInstanceId || undefined);
    }
  }

  function openExpSettingsModal() {
    if (!selectedPair) return;
    setProfileModalSection("exp");
    setProfileModalOpen(true);
    if (!profileDraft) {
      void loadProfile(selectedPair, selectedInstanceId || undefined);
    }
  }

  function discardProfileModal() {
    setProfileModalOpen(false);
    if (profileDirty && selectedPair) {
      void loadProfile(selectedPair, selectedInstanceId || undefined);
    }
  }

  function onProfileBackdropClick() {
    if (profileDirty) {
      setTrainerError("You have unsaved Pair Profile changes. Click Save or Discard.");
      return;
    }
    discardProfileModal();
  }

  async function saveProfileFromModal() {
    const ok = await saveProfile();
    if (ok) {
      setProfileModalOpen(false);
    }
  }

  function renderProfileModalActions() {
    return (
      <div className="ml-subpanel-actions ml-profile-modal-actions">
        <button type="button" onClick={() => void saveProfileFromModal()} disabled={trainerBusy || !profileDraft || !profileDirty}>
          Save
        </button>
        <button type="button" onClick={discardProfileModal} disabled={trainerBusy}>
          Discard
        </button>
        <span className={`ml-dirty ${profileDirty ? "dirty" : ""}`}>{profileDirty ? "Unsaved profile changes" : "Profile saved"}</span>
      </div>
    );
  }

  function exportProfileDraft() {
    if (!profileDraft || !selectedPair) {
      setTrainerError("Select a pair profile before exporting.");
      return;
    }
    const payload = {
      format: "futures_terminal_ml_profile",
      version: 1,
      exported_at: new Date().toISOString(),
      pair_symbol: selectedPair,
      profile: {
        ...profileDraft,
        pair_symbol: selectedPair,
      },
    };
    const json = JSON.stringify(payload, null, 2);
    const blob = new Blob([json], { type: "application/json;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const ts = new Date().toISOString().replace(/[:.]/g, "-");
    const filename = `${selectedPair}_pair_profile_${ts}.json`;
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
    setTrainerError("");
    setTrainerStatus(`Exported pair profile: ${filename}`);
  }

  function openImportProfilePicker() {
    profileImportInputRef.current?.click();
  }

  function handleImportProfileFile(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      try {
        const raw = JSON.parse(String(reader.result || "{}")) as Record<string, unknown>;
        const candidate = (raw.profile as Partial<MlProfile> | undefined) ?? (raw as Partial<MlProfile>);
        if (!candidate || typeof candidate !== "object") {
          throw new Error("Invalid profile file format.");
        }
        const targetPair = selectedPair || String(candidate.pair_symbol || "").trim().toUpperCase();
        if (!targetPair) {
          throw new Error("Could not resolve pair symbol from imported file.");
        }
        const merged = mergeProfile(targetPair, candidate);
        const allowedExchangeIds = new Set(allExchangeOptions.map((item) => item.exchange_id));
        merged.selected_exchanges = (merged.selected_exchanges || []).filter((id) => allowedExchangeIds.has(id));
        merged.training_selected_exchanges = (merged.training_selected_exchanges || []).filter((id) =>
          allowedExchangeIds.has(id)
        );
        merged.bot_data_selected_exchanges = (merged.bot_data_selected_exchanges || []).filter((id) =>
          allowedExchangeIds.has(id)
        );
        merged.bot_execution_selected_exchanges = (merged.bot_execution_selected_exchanges || []).filter((id) =>
          allowedExchangeIds.has(id)
        );
        merged.pair_symbol = targetPair;
        setProfileDraft(merged);
        setTrainerError("");
        setTrainerStatus(`Imported profile draft for ${targetPair}. Click Save to apply.`);
      } catch (err) {
        setTrainerError(err instanceof Error ? err.message : "Profile import failed.");
      } finally {
        event.target.value = "";
      }
    };
    reader.onerror = () => {
      setTrainerError("Could not read selected profile file.");
      event.target.value = "";
    };
    reader.readAsText(file, "utf-8");
  }

  const runListSelected = runs.find((item) => item.run_id === selectedRunId) ?? null;
  const selectedRunSummary = useMemo(() => {
    if (!runDetails && !runListSelected) return null;
    if (!runDetails) return runListSelected;
    if (!runListSelected) return runDetails;
    // Prefer live run-list lifecycle fields to avoid stale detail snapshots.
    return {
      ...runDetails,
      status: runListSelected.status,
      stage: runListSelected.stage,
      stage_progress: runListSelected.stage_progress,
      updated_at: runListSelected.updated_at,
      ended_at: runListSelected.ended_at,
    };
  }, [runDetails, runListSelected]);
  const selectedRunMetrics = useMemo(
    () => ((selectedRunSummary?.metrics ?? {}) as Record<string, unknown>),
    [selectedRunSummary]
  );
  const hasRecommendedGates = useMemo(() => {
    const conf = Number(selectedRunMetrics["recommended_confidence_threshold"]);
    const qual = Number(selectedRunMetrics["recommended_quality_threshold"]);
    const gap = Number(selectedRunMetrics["recommended_confidence_gap"]);
    return Number.isFinite(conf) && Number.isFinite(qual) && Number.isFinite(gap);
  }, [selectedRunMetrics]);
  const approvalGateStatus = useMemo(() => {
    const targetMode = String((selectedRunSummary?.config as Record<string, unknown> | undefined)?.target_mode ?? "triple_barrier").toLowerCase();
    const modelApprovalStatus = String(selectedRunMetrics["model_approval_status"] ?? "");
    const deployAllowed = Boolean(selectedRunMetrics["deploy_allowed"] ?? false);
    const approvalOk = Boolean(selectedRunMetrics["approval_ok"] ?? false);
    const reason = String(selectedRunMetrics["approval_reason"] ?? "");
    if (targetMode !== "trade_outcome") {
      return { canApprove: true, blockedReason: "" };
    }
    const canApprove = modelApprovalStatus === "APPROVABLE" && deployAllowed && approvalOk;
    return { canApprove, blockedReason: canApprove ? "" : (reason || "validation_overfit_or_test_failed") };
  }, [selectedRunMetrics, selectedRunSummary]);
  const telemetryNumber = (key: string): number | null => {
    const value = Number(selectedRunMetrics[key]);
    return Number.isFinite(value) ? value : null;
  };
  const hasQueuedOrRunningRun = useMemo(
    () =>
      runs.some((item) => {
        if (stopRequestedRunId && item.run_id === stopRequestedRunId) {
          return false;
        }
        return item.status === "QUEUED" || item.status === "RUNNING";
      }),
    [runs, stopRequestedRunId],
  );
  const canPauseOrStopSelectedRun = useMemo(() => {
    if (!selectedRunSummary) return false;
    return (
      selectedRunSummary.status === "QUEUED" ||
      selectedRunSummary.status === "RUNNING" ||
      selectedRunSummary.status === "PAUSED"
    );
  }, [selectedRunSummary]);
  useEffect(() => {
    if (!stopRequestedRunId) return;
    const current = runs.find((item) => item.run_id === stopRequestedRunId);
    if (!current || current.status === "STOPPED" || current.status === "FAILED" || current.status === "COMPLETED") {
      setStopRequestedRunId("");
    }
  }, [runs, stopRequestedRunId]);
  const stageOrder = ["data", "features", "labeling", "training", "evaluation", "ready"];
  const selectedDataHours = Number(dataStatus?.selected_status?.data_hours ?? dataStatus?.data_hours ?? 0);
  const selectedRows = Number(dataStatus?.selected_status?.rows_total ?? dataStatus?.rows_total ?? 0);
  const totalLocalHoursAvailable = Number(dataStatus?.total_data_hours_available ?? selectedDataHours ?? 0);
  const backfillProgressPct = Math.max(0, Math.min(100, Math.round((backfillStatus?.progress ?? 0) * 100)));
  const backfillElapsedMs = useMemo(() => {
    if (!backfillStatus?.started_at) return 0;
    const started = Date.parse(backfillStatus.started_at);
    if (!Number.isFinite(started)) return 0;
    const status = String(backfillStatus.status ?? "").toUpperCase();
    const isTerminal = status === "COMPLETED" || status === "FAILED" || status === "STOPPED";
    const endSource = backfillStatus.ended_at || (isTerminal ? backfillStatus.updated_at : "");
    const end = endSource ? Date.parse(endSource) : Date.now();
    return Math.max(0, end - started);
  }, [backfillStatus?.started_at, backfillStatus?.ended_at, backfillStatus?.updated_at, backfillStatus?.status]);
  const backfillEtaMs = useMemo(() => {
    if (!backfillStatus) return null;
    if (backfillStatus.status !== "RUNNING") return null;
    const progress = Math.max(0, Math.min(1, backfillStatus.progress ?? 0));
    if (progress <= 0.01 || progress >= 0.999) return null;
    const estimate = (backfillElapsedMs * (1 - progress)) / progress;
    if (!Number.isFinite(estimate) || estimate <= 0) return null;
    return Math.round(estimate);
  }, [backfillStatus, backfillElapsedMs]);

  const runActionProgressPct = useMemo(() => {
    if (!runActionProgress) return 0;
    if (runActionProgress.state === "done") return 100;
    const elapsed = Math.max(0, runActionNowMs - runActionProgress.startedAtMs);
    const ratio = elapsed / Math.max(1, runActionProgress.etaMs);
    return Math.max(2, Math.min(95, Math.round(ratio * 100)));
  }, [runActionNowMs, runActionProgress]);

  const runActionElapsedMs = useMemo(() => {
    if (!runActionProgress) return 0;
    if (runActionProgress.state === "done") return runActionProgress.etaMs;
    return Math.max(0, runActionNowMs - runActionProgress.startedAtMs);
  }, [runActionNowMs, runActionProgress]);

  const runActionEtaMs = useMemo(() => {
    if (!runActionProgress || runActionProgress.state === "done") return 0;
    return Math.max(0, runActionProgress.etaMs - runActionElapsedMs);
  }, [runActionElapsedMs, runActionProgress]);

  const runActionTitle = useMemo(() => {
    if (!runActionProgress) return "";
    const map: Record<RunActionName, string> = {
      start: "Starting Run",
      pause: "Pausing Run",
      stop: "Stopping Run",
    };
    return map[runActionProgress.action];
  }, [runActionProgress]);

  useEffect(() => {
    if (workloadControls?.training_enabled) {
      setAutoStopSuppressed(false);
    }
  }, [workloadControls?.training_enabled]);

  useEffect(() => {
    if (!workloadControls || workloadControls.training_enabled) return;
    if (!selectedRunId || !selectedRunSummary) return;
    if (!(selectedRunSummary.status === "QUEUED" || selectedRunSummary.status === "RUNNING" || selectedRunSummary.status === "PAUSED")) return;
    if (autoStopSuppressed) return;
    if (trainerBusy) return;
    void actionStopRun();
  }, [workloadControls, selectedRunId, selectedRunSummary, trainerBusy, autoStopSuppressed]);

  useEffect(() => {
    if (!workloadControls || workloadControls.bots_enabled) return;
    const status = String(botStatus?.status ?? "").toUpperCase();
    if (!selectedPair || status === "IDLE" || status === "STOPPED") return;
    if (trainerBusy) return;
    void actionBotStop();
  }, [workloadControls, botStatus?.status, selectedPair, trainerBusy]);

  useEffect(() => {
    if (!workloadControls || workloadControls.backfill_enabled) return;
    const status = String(backfillStatus?.status ?? "").toUpperCase();
    if (!backfillJobId || (status !== "RUNNING" && status !== "PAUSED")) return;
    if (trainerBusy) return;
    void actionBackfillStop();
  }, [workloadControls, backfillStatus?.status, backfillJobId, trainerBusy]);

  async function loadStorageOverview() {
    setStorageLoading(true);
    setStorageError("");
    try {
      let response = await fetch(`${API_BASE}/settings/storage/overview`, { cache: "no-store" });
      if (response.status === 404) {
        response = await fetch(`${API_BASE}/ml/storage/overview`, { cache: "no-store" });
      }
      if (!response.ok) {
        throw new Error(`Storage overview failed (${response.status})`);
      }
      const payload = (await response.json()) as Partial<StorageOverview>;
      const categories = Array.isArray(payload?.categories) ? payload.categories : null;
      const totals = payload?.totals as StorageOverview["totals"] | undefined;
      if (!categories || !totals || typeof totals.category_count !== "number") {
        throw new Error("Storage overview returned unexpected payload. Restart backend to load latest routes.");
      }
      setStorageOverview(payload as StorageOverview);
      setStorageSelectedIds(new Set());
    } catch (err) {
      setStorageError(err instanceof Error ? err.message : "Could not load storage overview.");
    } finally {
      setStorageLoading(false);
    }
  }

  useEffect(() => {
    if (settingsView !== "storage_management") return;
    void loadStorageOverview();
  }, [settingsView]);

  useEffect(() => {
    if (settingsView !== "storage_management") {
      setStorageSubView("storage_hub");
      setSelectedStoragePair("");
      setPairDataSelectedKinds(new Set());
      return;
    }
    setStorageSubView("storage_hub");
  }, [settingsView]);

  const storageEntries = useMemo(() => {
    const categories = storageOverview?.categories ?? [];
    return categories.flatMap((category) =>
      (category.entries ?? []).map((entry) => ({ ...entry, category_label: category.label }))
    );
  }, [storageOverview]);

  const selectedStorageEntries = useMemo(
    () => storageEntries.filter((entry) => storageSelectedIds.has(entry.entry_id)),
    [storageEntries, storageSelectedIds]
  );

  const storagePerPair = useMemo<PairStorageSummary[]>(() => {
    const categories = storageOverview?.categories ?? [];
    const byCategory = new Map<string, StorageEntry[]>();
    for (const category of categories) {
      byCategory.set(category.category_id, Array.isArray(category.entries) ? category.entries : []);
    }
    const allEntries = categories.flatMap((category) => category.entries ?? []);
    const pairs = Array.from(
      new Set(
        allEntries
          .map((entry) => String(entry.pair_symbol || "").toUpperCase())
          .filter((pair) => pair && pair !== "UNSCOPED")
      )
    ).sort((a, b) => a.localeCompare(b));

    function uniqueEntries(entries: StorageEntry[]) {
      const seen = new Set<string>();
      const out: StorageEntry[] = [];
      for (const entry of entries) {
        if (seen.has(entry.entry_id)) continue;
        seen.add(entry.entry_id);
        out.push(entry);
      }
      return out;
    }

    function fromCategory(categoryId: string, pair: string, matcher?: (entry: StorageEntry) => boolean): StorageEntry[] {
      return uniqueEntries(
        (byCategory.get(categoryId) ?? []).filter((entry) => {
          const entryPair = String(entry.pair_symbol || "").toUpperCase();
          if (entryPair !== pair) return false;
          return matcher ? matcher(entry) : true;
        })
      );
    }

    return pairs
      .map((pairSymbol) => {
        const kinds: PairDataKindItem[] = [];

        const replayEntries = fromCategory("replay_sessions", pairSymbol);
        if (replayEntries.length) {
          kinds.push({
            kind: "live_recording_files_replays",
            label: "Live recording files (replays)",
            sizeBytes: replayEntries.reduce((sum, entry) => sum + Number(entry.size_bytes || 0), 0),
            entries: replayEntries,
          });
        }

        const marketEventEntries = fromCategory("market_event_recordings", pairSymbol);
        if (marketEventEntries.length) {
          kinds.push({
            kind: "market_event_full_fidelity_chunks",
            label: "Market event recordings (full fidelity)",
            sizeBytes: marketEventEntries.reduce((sum, entry) => sum + Number(entry.size_bytes || 0), 0),
            entries: marketEventEntries,
          });
        }

        const liveRawEntries = fromCategory(
          "live_recorded_data",
          pairSymbol,
          (entry) => String((entry.delete_target as { group?: string })?.group || "") === "raw_events"
        );
        if (liveRawEntries.length) {
          kinds.push({
            kind: "live_raw_events_terminal_rows",
            label: "Live raw events (terminal.db rows)",
            sizeBytes: 0,
            dbScoped: true,
            entries: liveRawEntries,
          });
        }

        const featuresEntries = fromCategory("training_features", pairSymbol);
        if (featuresEntries.length) {
          kinds.push({
            kind: "training_features_disk",
            label: "Training features (disk)",
            sizeBytes: featuresEntries.reduce((sum, entry) => sum + Number(entry.size_bytes || 0), 0),
            entries: featuresEntries,
          });
        }

        const modelEntries = fromCategory(
          "training_runs_models",
          pairSymbol,
          (entry) => entry.type !== "db_group" && String(entry.path || "").toUpperCase().includes(`\\MODELS\\${pairSymbol}`)
        );
        if (modelEntries.length) {
          kinds.push({
            kind: "models_disk",
            label: "Models (disk)",
            sizeBytes: modelEntries.reduce((sum, entry) => sum + Number(entry.size_bytes || 0), 0),
            entries: modelEntries,
          });
        }

        const runsEntries = fromCategory(
          "training_runs_models",
          pairSymbol,
          (entry) =>
            entry.type === "db_group" &&
            String((entry.delete_target as { group?: string })?.group || "").toLowerCase() === "training_runs"
        );
        if (runsEntries.length) {
          kinds.push({
            kind: "ml_runs",
            label: "ML runs",
            sizeBytes: 0,
            dbScoped: true,
            entries: runsEntries,
          });
          kinds.push({
            kind: "trace_frames",
            label: "Trace frames",
            sizeBytes: 0,
            dbScoped: true,
            entries: runsEntries,
          });
          kinds.push({
            kind: "historic_manifests",
            label: "Historic manifests",
            sizeBytes: 0,
            dbScoped: true,
            entries: runsEntries,
          });
        }

        const botTradeEntries = fromCategory(
          "bot_data",
          pairSymbol,
          (entry) =>
            entry.type === "db_group" &&
            String((entry.delete_target as { group?: string })?.group || "").toLowerCase() === "bot_data"
        );
        if (botTradeEntries.length) {
          kinds.push({
            kind: "bot_trades",
            label: "Bot trades",
            sizeBytes: 0,
            dbScoped: true,
            entries: botTradeEntries,
          });
        }

        const botSessionEntries = fromCategory(
          "sessions",
          pairSymbol,
          (entry) =>
            entry.type === "db_group" &&
            String((entry.delete_target as { group?: string })?.group || "").toLowerCase() === "sessions"
        );
        if (botSessionEntries.length) {
          kinds.push({
            kind: "bot_sessions",
            label: "Bot sessions",
            sizeBytes: 0,
            dbScoped: true,
            entries: botSessionEntries,
          });
        }

        const acquiredHistoricEntries = fromCategory("history_backfill", pairSymbol);
        if (acquiredHistoricEntries.length) {
          kinds.push({
            kind: "acquired_historic_data",
            label: "Acquired historic data",
            sizeBytes: acquiredHistoricEntries.reduce((sum, entry) => sum + Number(entry.size_bytes || 0), 0),
            entries: acquiredHistoricEntries,
          });
        }

        const totalSizeBytes = kinds.reduce((sum, kind) => sum + Number(kind.sizeBytes || 0), 0);
        return {
          pairSymbol,
          totalSizeBytes,
          kinds,
        };
      })
      .filter((item) => item.kinds.length > 0);
  }, [storageOverview]);

  const selectedPairStorage = useMemo(
    () => storagePerPair.find((item) => item.pairSymbol === selectedStoragePair) ?? null,
    [storagePerPair, selectedStoragePair]
  );

  function toggleStorageEntry(entryId: string, checked: boolean) {
    setStorageSelectedIds((prev) => {
      const next = new Set(prev);
      if (checked) next.add(entryId);
      else next.delete(entryId);
      return next;
    });
  }

  function togglePairDataKind(kind: PairDataKindId, checked: boolean) {
    setPairDataSelectedKinds((prev) => {
      const next = new Set(prev);
      if (checked) next.add(kind);
      else next.delete(kind);
      return next;
    });
  }

  function openPairStorageDetail(pairSymbol: string) {
    setSelectedStoragePair(pairSymbol);
    setPairDataSelectedKinds(new Set());
    setStorageSubView("storage_pair_detail");
  }

  function backFromStorageView() {
    if (storageSubView === "storage_hub") {
      onSettingsViewChange("hub");
      return;
    }
    if (storageSubView === "storage_pair_detail") {
      setPairDataSelectedKinds(new Set());
      setStorageSubView("storage_per_pair");
      return;
    }
    setStorageSubView("storage_hub");
  }

  function openDeleteModalForSelectedPairKinds() {
    if (!selectedPairStorage) return;
    const selectedKinds = selectedPairStorage.kinds.filter((kind) => pairDataSelectedKinds.has(kind.kind));
    if (!selectedKinds.length) return;
    const entries = selectedKinds.flatMap((kind) => kind.entries);
    const byEntryId = new Map<string, StorageEntry>();
    for (const entry of entries) {
      if (!byEntryId.has(entry.entry_id)) {
        byEntryId.set(entry.entry_id, {
          ...entry,
          name: `${selectedPairStorage.pairSymbol} - ${selectedKinds.find((kind) => kind.entries.some((it) => it.entry_id === entry.entry_id))?.label || entry.name}`,
        });
      }
    }
    openDeleteModalForEntries(Array.from(byEntryId.values()));
  }

  function openDeleteModalForEntries(entries: StorageEntry[]) {
    if (!entries.length) return;
    setStorageDeleteTargets(entries);
    setStorageDeleteOpen(true);
  }

  function closeDeleteModal() {
    if (storageBusy) return;
    setStorageDeleteOpen(false);
    setStorageDeleteTargets([]);
  }

  async function confirmStorageDelete() {
    if (!storageDeleteTargets.length) return;
    setStorageBusy(true);
    setStorageDeleteProgress("");
    setStorageDeleteJobId("");
    setStorageError("");
    setStorageStatus("");
    try {
      let response = await fetch(`${API_BASE}/settings/storage/delete/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ targets: storageDeleteTargets.map((entry) => entry.delete_target) }),
      });
      if (response.status === 404) {
        response = await fetch(`${API_BASE}/settings/storage/delete`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ targets: storageDeleteTargets.map((entry) => entry.delete_target) }),
        });
      }
      const payload = (await response.json()) as {
        job_id?: string;
        status?: string;
        total_targets?: number;
        deleted_count?: number;
        failed_count?: number;
        reclaimed_bytes?: number;
        failures?: { target: string; error: string }[];
      };
      if (!response.ok) {
        throw new Error((payload as { detail?: string }).detail || `Delete failed (${response.status})`);
      }
      if (typeof payload.job_id === "string" && payload.job_id.trim()) {
        const jobId = payload.job_id.trim();
        setStorageDeleteJobId(jobId);
        setStorageDeleteProgress("Delete job queued...");
        const deadlineMs = Date.now() + 15 * 60_000;
        while (Date.now() < deadlineMs) {
          await new Promise((resolve) => window.setTimeout(resolve, 600));
          const statusResponse = await fetch(`${API_BASE}/settings/storage/delete/jobs/${encodeURIComponent(jobId)}`, { cache: "no-store" });
          const jobPayload = (await statusResponse.json()) as {
            status?: string;
            total_targets?: number;
            completed_targets?: number;
            error?: string;
            result?: {
              deleted_count?: number;
              failed_count?: number;
              reclaimed_bytes?: number;
              failures?: { target: string; error: string }[];
            };
          };
          if (!statusResponse.ok) {
            throw new Error((jobPayload as { detail?: string }).detail || `Delete status failed (${statusResponse.status})`);
          }
          const totalTargets = Number(jobPayload.total_targets ?? storageDeleteTargets.length);
          const completedTargets = Number(jobPayload.completed_targets ?? 0);
          setStorageDeleteProgress(`Deleting... ${Math.min(completedTargets, totalTargets)}/${Math.max(1, totalTargets)} target(s)`);
          const state = String(jobPayload.status || "").toLowerCase();
          if (state === "completed") {
            const result = jobPayload.result ?? {};
            const deletedCount = Number(result.deleted_count ?? 0);
            const failedCount = Number(result.failed_count ?? 0);
            const reclaimed = Number(result.reclaimed_bytes ?? 0);
            const failureSuffix =
              failedCount > 0
                ? ` Failed: ${failedCount}${result.failures?.length ? ` (${result.failures[0].error})` : ""}.`
                : "";
            setStorageStatus(`Deleted ${deletedCount} target(s). Reclaimed ${formatBytes(reclaimed)}.${failureSuffix}`);
            setStorageDeleteProgress("");
            setStorageDeleteOpen(false);
            setStorageDeleteTargets([]);
            await loadStorageOverview();
            setPairDataSelectedKinds(new Set());
            return;
          }
          if (state === "failed") {
            throw new Error(jobPayload.error || "Delete job failed.");
          }
        }
        throw new Error("Delete job timed out while waiting for completion.");
      }
      const deletedCount = Number(payload.deleted_count ?? 0);
      const failedCount = Number(payload.failed_count ?? 0);
      const reclaimed = Number(payload.reclaimed_bytes ?? 0);
      const failureSuffix =
        failedCount > 0
          ? ` Failed: ${failedCount}${payload.failures?.length ? ` (${payload.failures[0].error})` : ""}.`
          : "";
      setStorageStatus(`Deleted ${deletedCount} target(s). Reclaimed ${formatBytes(reclaimed)}.${failureSuffix}`);
      setStorageDeleteOpen(false);
      setStorageDeleteTargets([]);
      await loadStorageOverview();
      setPairDataSelectedKinds(new Set());
    } catch (err) {
      setStorageError(err instanceof Error ? err.message : "Storage delete failed.");
    } finally {
      setStorageDeleteJobId("");
      setStorageDeleteProgress("");
      setStorageBusy(false);
    }
  }

  function handleWorkloadToggle(key: WorkloadToggleKey, checked: boolean) {
    if (!workloadControls || !onWorkloadControlsChange) return;
    onWorkloadControlsChange({ ...workloadControls, [key]: checked });
  }

  function handleRamBudgetChange(value: string) {
    if (!workloadControls || !onWorkloadControlsChange) return;
    const trimmed = value.trim();
    if (!trimmed) {
      onWorkloadControlsChange({ ...workloadControls, training_ram_budget_gb: null });
      return;
    }
    const parsed = Number(trimmed);
    if (!Number.isFinite(parsed)) return;
    const clamped = Math.max(0.5, Math.min(64, parsed));
    onWorkloadControlsChange({ ...workloadControls, training_ram_budget_gb: clamped });
  }

  function handlePrefetchToggle(checked: boolean) {
    if (!workloadControls || !onWorkloadControlsChange) return;
    onWorkloadControlsChange({ ...workloadControls, training_prefetch_enabled: checked });
  }

  function handleTrainingChunkGroupSizeChange(value: string) {
    if (!workloadControls || !onWorkloadControlsChange) return;
    const normalized = String(value || "auto").trim().toLowerCase();
    const allowed = new Set(["auto", "1", "2", "3", "5", "8", "10"]);
    const safe = allowed.has(normalized) ? normalized : "auto";
    const nextValue = safe === "auto" ? "auto" : (Number(safe) as 1 | 2 | 3 | 5 | 8 | 10);
    onWorkloadControlsChange({ ...workloadControls, training_chunk_group_size: nextValue });
  }

  function setToolMatrixCell(rowId: ToolRowId, mode: ToolMode, checked: boolean) {
    setToolModeMatrixDraft((prev) => ({
      ...prev,
      rows: {
        ...prev.rows,
        [rowId]: {
          ...prev.rows[rowId],
          [mode]: checked,
        },
      },
    }));
  }

  function setToolMatrixAll(checked: boolean) {
    setToolModeMatrixDraft((prev) => {
      const nextRows = { ...prev.rows };
      for (const row of TOOL_ROW_META) {
        nextRows[row.id] = {
          live: checked,
          replay: checked,
          recording: checked,
          training: checked,
          bot: checked,
        };
      }
      return { ...prev, rows: nextRows };
    });
  }

  function setToolMatrixColumn(mode: ToolMode, checked: boolean) {
    setToolModeMatrixDraft((prev) => {
      const nextRows = { ...prev.rows };
      for (const row of TOOL_ROW_META) {
        nextRows[row.id] = {
          ...nextRows[row.id],
          [mode]: checked,
        };
      }
      return { ...prev, rows: nextRows };
    });
  }

  async function loadToolModeMatrix() {
    try {
      const response = await fetch(`${API_BASE}/settings/tool-mode-matrix`, { cache: "no-store" });
      if (!response.ok) {
        throw new Error(`Failed to load Manage Tools matrix (${response.status})`);
      }
      const payload = normalizeToolModeMatrix(await response.json());
      setToolModeMatrixSaved(payload);
      setToolModeMatrixDraft(payload);
    } catch (err) {
      setServiceSettingsError(err instanceof Error ? err.message : "Could not load Manage Tools matrix.");
    }
  }

  async function applyToolModeMatrix() {
    setToolModeMatrixApplying(true);
    setServiceSettingsError("");
    try {
      const response = await fetch(`${API_BASE}/settings/tool-mode-matrix`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rows: toolModeMatrixDraft.rows }),
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(String(payload?.detail || `Failed to apply Manage Tools matrix (${response.status})`));
      }
      const normalized = normalizeToolModeMatrix(payload);
      setToolModeMatrixSaved(normalized);
      setToolModeMatrixDraft(normalized);
      setServiceSettingsStatus("Manage Tools matrix applied.");
    } catch (err) {
      setServiceSettingsError(err instanceof Error ? err.message : "Could not apply Manage Tools matrix.");
    } finally {
      setToolModeMatrixApplying(false);
    }
  }

  function handleRecordingModeSelect(value: string) {
    if (!onRecordingModeChange) return;
    const next: RecordingMode = value === "full_fidelity" ? "full_fidelity" : "lightweight";
    onRecordingModeChange(next);
  }

  async function loadRecordingState() {
    try {
      let response = await fetch(`${API_BASE}/settings/recording-state`, { cache: "no-store" });
      if (response.status === 404) {
        response = await fetch(`${API_BASE}/settings/recording-mode`, { cache: "no-store" });
      }
      if (!response.ok) return;
      const payload = (await response.json()) as {
        recording?: { soft_cap_pairs_blocked?: string[]; enabled?: boolean };
        enabled?: boolean;
      };
      const enabledRaw =
        typeof payload?.enabled === "boolean"
          ? payload.enabled
          : typeof payload?.recording?.enabled === "boolean"
            ? payload.recording.enabled
            : null;
      if (enabledRaw != null) {
        setRecordingEnabled(enabledRaw);
      }
      const blocked = Array.isArray(payload?.recording?.soft_cap_pairs_blocked)
        ? payload.recording.soft_cap_pairs_blocked.map((item) => String(item).toUpperCase()).filter((item) => item.length > 0)
        : [];
      setRecordingSoftCapBlockedPairs(blocked);
      if (!blocked.length) {
        setServiceSettingsStatus("");
        setServiceSettingsError("");
      }
    } catch {
      // ignore transient refresh errors
    }
  }

  async function toggleRecordingEnabled(nextEnabled: boolean) {
    const pair = normalizePair(selectedPair || profileDraft?.pair_symbol || "XRPUSDT");
    setRecordingToggleBusy(true);
    setServiceSettingsError("");
    setServiceSettingsStatus("");
    try {
      const response = await fetch(`${API_BASE}/settings/recording-state`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ active_pair_symbol: pair, enabled: nextEnabled }),
      });
      const payload = (await response.json().catch(() => ({}))) as {
        detail?: string;
        enabled?: boolean;
        recording?: { enabled?: boolean };
      };
      if (!response.ok) {
        throw new Error(payload?.detail || `Failed to update recording state (${response.status})`);
      }
      const enabledRaw =
        typeof payload?.enabled === "boolean"
          ? payload.enabled
          : typeof payload?.recording?.enabled === "boolean"
            ? payload.recording.enabled
            : nextEnabled;
      setRecordingEnabled(enabledRaw);
      setServiceSettingsStatus(nextEnabled ? `Recording started for ${pair}.` : `Recording stopped for ${pair}.`);
      void loadRecordingState();
    } catch (err) {
      setServiceSettingsError(err instanceof Error ? err.message : "Could not update recording state.");
    } finally {
      setRecordingToggleBusy(false);
    }
  }

  async function allowSoftCapRecording(pairSymbol: string) {
    const pair = normalizePair(pairSymbol);
    const confirmed = window.confirm(
      `Allow full-fidelity recording to continue for ${pair} beyond soft storage cap?\n\nThis may increase disk usage quickly.`,
    );
    if (!confirmed) return;
    setRecordingSoftCapOverrideBusyPair(pair);
    setServiceSettingsError("");
    setServiceSettingsStatus("");
    try {
      const response = await fetch(`${API_BASE}/settings/recording-soft-cap`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ pair_symbol: pair, allow_continue: true }),
      });
      const payload = (await response.json()) as {
        recording?: { soft_cap_pairs_blocked?: string[] };
        detail?: string;
      };
      if (!response.ok) {
        throw new Error(payload.detail || `Failed to apply soft-cap override (${response.status})`);
      }
      const blocked = Array.isArray(payload?.recording?.soft_cap_pairs_blocked)
        ? payload.recording.soft_cap_pairs_blocked.map((item) => String(item).toUpperCase()).filter((item) => item.length > 0)
        : [];
      setRecordingSoftCapBlockedPairs(blocked);
      setServiceSettingsStatus(`Soft-cap override enabled for ${pair}.`);
    } catch (err) {
      setServiceSettingsError(err instanceof Error ? err.message : "Could not apply soft-cap override.");
    } finally {
      setRecordingSoftCapOverrideBusyPair("");
    }
  }

  async function runMarketEventCompaction() {
    const pair = normalizePair(selectedPair || profileDraft?.pair_symbol || "XRPUSDT");
    const confirmed = window.confirm(
      `Run market-event compaction for ${pair} now?\n\nThis can take time, but usually speeds up full-fidelity training.`,
    );
    if (!confirmed) return;
    setCompactionBusy(true);
    setServiceSettingsError("");
    setServiceSettingsStatus("");
    try {
      const response = await fetch(`${API_BASE}/settings/storage/market-events/compact`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ pair_symbol: pair }),
      });
      const payload = (await response.json()) as {
        compacted?: boolean;
        files_before?: number;
        files_after?: number;
        detail?: string;
      };
      if (!response.ok) {
        throw new Error(payload.detail || `Compaction failed (${response.status})`);
      }
      if (payload.compacted) {
        setServiceSettingsStatus(
          `Compaction completed for ${pair}: files ${Number(payload.files_before || 0)} -> ${Number(payload.files_after || 0)}.`,
        );
      } else {
        setServiceSettingsStatus(`Compaction skipped for ${pair} (${String(payload.detail || "no changes")}).`);
      }
    } catch (err) {
      setServiceSettingsError(err instanceof Error ? err.message : "Could not run compaction.");
    } finally {
      setCompactionBusy(false);
    }
  }

  useEffect(() => {
    if (settingsView !== "service_settings") return;
    void loadRecordingState();
    void loadToolModeMatrix();
  }, [settingsView, recordingMode]);

  const toolModeMatrixDirty = useMemo(
    () => JSON.stringify(toolModeMatrixDraft.rows) !== JSON.stringify(toolModeMatrixSaved.rows),
    [toolModeMatrixDraft, toolModeMatrixSaved],
  );

  return (
    <>
      {settingsView === "hub" ? (
        <section className="panel">
          <div className="small-title">Settings</div>
          <p>Choose a section to open full settings view.</p>
          <div className="settings-hub-grid">
            <button type="button" className="settings-hub-card" onClick={() => onSettingsViewChange("workspace_presets")}>
              <strong>Workspace Presets</strong>
              <span>Chart/layout presets and heatmap defaults.</span>
            </button>
            <button
              type="button"
              className="settings-hub-card"
              onClick={() => {
                onSettingsViewChange("trainer_bot");
                setMlDetailsOpen(false);
              }}
            >
              <strong>Trainer &amp; Bot</strong>
              <span>Model training, runs, bot sessions and profile controls.</span>
            </button>
            <button type="button" className="settings-hub-card" onClick={() => onSettingsViewChange("service_settings")}>
              <strong>Service Settings</strong>
              <span>Enable/disable live, replay, training, bot and backfill services.</span>
            </button>
            <button type="button" className="settings-hub-card" onClick={() => onSettingsViewChange("storage_management")}>
              <strong>Storage Management</strong>
              <span>Browse user data by category/pair and permanently delete selected storage targets.</span>
            </button>
          </div>
        </section>
      ) : null}

      {settingsView === "service_settings" ? (
        <section className="panel">
          <div className="settings-section-topbar">
            <button type="button" onClick={() => onSettingsViewChange("hub")}>Back</button>
          </div>
          <div className="small-title">Service Settings</div>
          {lowLoadDisabledServices.length > 0 ? (
            <div className="settings-load-indicator active">
              Low-load active for: {lowLoadDisabledServices.join(", ")}
            </div>
          ) : (
            <div className="settings-load-indicator">Normal-load mode (all services enabled)</div>
          )}
          <p>Disable modules you are not using so the app can dedicate resources to your active task.</p>
          <div className="ml-toolbar" style={{ marginBottom: 10 }}>
            <label>
              Recording Mode
              <select
                value={recordingMode === "full_fidelity" ? "full_fidelity" : "lightweight"}
                onChange={(event) => handleRecordingModeSelect(event.target.value)}
              >
                <option value="lightweight">Lightweight (sampled)</option>
                <option value="full_fidelity">Full Fidelity (chunked parquet)</option>
              </select>
            </label>
            <button type="button" onClick={() => void toggleRecordingEnabled(!recordingEnabled)} disabled={recordingToggleBusy}>
              {recordingToggleBusy ? "Updating..." : recordingEnabled ? "Stop Recording" : "Start Recording"}
            </button>
            <span
              style={{
                alignSelf: "center",
                padding: "6px 10px",
                borderRadius: 999,
                border: `1px solid ${recordingEnabled ? "#18a957" : "#b23b3b"}`,
                color: recordingEnabled ? "#8ef0b2" : "#ff9d9d",
                fontSize: 12,
                whiteSpace: "nowrap",
              }}
            >
              Recording: {recordingEnabled ? "ON" : "OFF"}
            </span>
            <button type="button" onClick={() => void runMarketEventCompaction()} disabled={compactionBusy}>
              {compactionBusy ? "Compacting..." : "Compact Now"}
            </button>
          </div>
          {serviceSettingsStatus ? <div className="settings-status">{serviceSettingsStatus}</div> : null}
          {serviceSettingsError ? <div className="coverage-error">{serviceSettingsError}</div> : null}
          {recordingMode === "full_fidelity" && recordingSoftCapBlockedPairs.length > 0 ? (
            <section className="panel" style={{ marginBottom: 10 }}>
              <div className="small-title">Soft Cap Warnings</div>
              <p>These pairs are paused at storage soft cap. Confirm to continue recording without auto-delete.</p>
              <div style={{ display: "grid", gap: 8 }}>
                {recordingSoftCapBlockedPairs.map((pair) => (
                  <div key={`softcap-${pair}`} className="kv">
                    <span>{pair}</span>
                    <button
                      type="button"
                      onClick={() => void allowSoftCapRecording(pair)}
                      disabled={recordingSoftCapOverrideBusyPair === pair}
                    >
                      {recordingSoftCapOverrideBusyPair === pair ? "Applying..." : "Allow Continue"}
                    </button>
                  </div>
                ))}
              </div>
            </section>
          ) : null}
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 8 }}>
            <label className="settings-checkbox-row">
              <input
                type="checkbox"
                checked={Boolean(workloadControls?.live_streaming_enabled)}
                onChange={(e) => handleWorkloadToggle("live_streaming_enabled", e.target.checked)}
              />
              Live Streaming
            </label>
            <label className="settings-checkbox-row">
              <input
                type="checkbox"
                checked={Boolean(workloadControls?.replay_enabled)}
                onChange={(e) => handleWorkloadToggle("replay_enabled", e.target.checked)}
              />
              Replay Engine
            </label>
            <label className="settings-checkbox-row">
              <input
                type="checkbox"
                checked={Boolean(workloadControls?.training_enabled)}
                onChange={(e) => handleWorkloadToggle("training_enabled", e.target.checked)}
              />
              Training Jobs
            </label>
            <label className="settings-checkbox-row">
              <input
                type="checkbox"
                checked={Boolean(workloadControls?.bots_enabled)}
                onChange={(e) => handleWorkloadToggle("bots_enabled", e.target.checked)}
              />
              Bot Engine
            </label>
            <label className="settings-checkbox-row">
              <input
                type="checkbox"
                checked={Boolean(workloadControls?.backfill_enabled)}
                onChange={(e) => handleWorkloadToggle("backfill_enabled", e.target.checked)}
              />
              Backfill Jobs
            </label>
            <label className="settings-checkbox-row">
              <input
                type="checkbox"
                checked={Boolean(workloadControls?.training_prefetch_enabled)}
                onChange={(e) => handlePrefetchToggle(e.target.checked)}
              />
              Training Prefetch
            </label>
            <label className="settings-checkbox-row" style={{ alignItems: "center", gap: 8 }}>
              <span style={{ minWidth: 140 }}>Training RAM Budget (GB)</span>
              <input
                type="number"
                min={0.5}
                max={64}
                step={0.1}
                value={
                  workloadControls?.training_ram_budget_gb == null
                    ? ""
                    : String(workloadControls.training_ram_budget_gb)
                }
                placeholder="Auto"
                onChange={(e) => handleRamBudgetChange(e.target.value)}
                style={{ width: 120 }}
              />
            </label>
            <label className="settings-checkbox-row" style={{ alignItems: "center", gap: 8 }}>
              <span style={{ minWidth: 140 }}>Chunks loaded at once</span>
              <select
                value={String(workloadControls?.training_chunk_group_size ?? "auto")}
                onChange={(e) => handleTrainingChunkGroupSizeChange(e.target.value)}
                title="Controls how many raw chunks each worker converts into disk shards at once. Parent stays metadata-only."
                style={{ width: 120 }}
              >
                <option value="auto">Auto</option>
                <option value="1">1</option>
                <option value="2">2</option>
                <option value="3">3</option>
                <option value="5">5</option>
                <option value="8">8</option>
                <option value="10">10</option>
              </select>
            </label>
            <label className="settings-checkbox-row">
              <input
                type="checkbox"
                checked={Boolean(workloadControls?.keep_training_shards)}
                onChange={(e) =>
                  onWorkloadControlsChange({
                    ...workloadControls,
                    keep_training_shards: e.target.checked,
                  })
                }
              />
              Keep Training Shards (debug)
            </label>
          </div>
          <section className="panel" style={{ marginTop: 12 }}>
            <div className="small-title">Manage Tools</div>
            <p>Control core stream tools by mode. Unchecked tools are hard-disabled for that mode.</p>
            <div className="coverage-bulk-actions">
              <button type="button" onClick={() => setToolMatrixAll(true)} disabled={toolModeMatrixApplying}>
                Check All
              </button>
              <button type="button" onClick={() => setToolMatrixAll(false)} disabled={toolModeMatrixApplying}>
                Uncheck All
              </button>
            </div>
            <div className="coverage-list">
              <div className="coverage-list-head">
                <span>Tool</span>
                {TOOL_MODE_COLUMNS.map((mode) => {
                  const label = mode.charAt(0).toUpperCase() + mode.slice(1);
                  const allChecked = TOOL_ROW_META.every((row) => Boolean(toolModeMatrixDraft.rows[row.id][mode]));
                  return (
                    <span key={`tool-col-head-${mode}`} style={{ display: "inline-flex", alignItems: "center", justifyContent: "center", gap: 6 }}>
                      <input
                        type="checkbox"
                        checked={allChecked}
                        onChange={(event) => setToolMatrixColumn(mode, event.target.checked)}
                        disabled={toolModeMatrixApplying}
                      />
                      {label}
                    </span>
                  );
                })}
              </div>
              {TOOL_ROW_META.map((row) => (
                <div key={`tool-row-${row.id}`} className="coverage-row">
                  <span>{row.label}</span>
                  {TOOL_MODE_COLUMNS.map((mode) => (
                    <label key={`${row.id}-${mode}`} className="coverage-row-left" style={{ justifyContent: "center" }}>
                      <input
                        type="checkbox"
                        checked={Boolean(toolModeMatrixDraft.rows[row.id][mode])}
                        onChange={(event) => setToolMatrixCell(row.id, mode, event.target.checked)}
                        disabled={toolModeMatrixApplying}
                      />
                    </label>
                  ))}
                </div>
              ))}
            </div>
            <div className="coverage-actions">
              <div className={`coverage-draft-note ${toolModeMatrixDirty ? "dirty" : ""}`}>
                {toolModeMatrixDirty ? "Unsaved changes" : "No pending changes"}
              </div>
              <div className="coverage-action-buttons">
                <button
                  type="button"
                  onClick={() => setToolModeMatrixDraft(toolModeMatrixSaved)}
                  disabled={toolModeMatrixApplying || !toolModeMatrixDirty}
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={() => void applyToolModeMatrix()}
                  disabled={toolModeMatrixApplying || !toolModeMatrixDirty}
                >
                  {toolModeMatrixApplying ? "Applying..." : "Apply"}
                </button>
              </div>
            </div>
          </section>
        </section>
      ) : null}

      {settingsView === "storage_management" ? (
        <section className="panel">
          <div className="settings-section-topbar">
            <button type="button" onClick={backFromStorageView}>Back</button>
          </div>
          <div className="small-title">Storage Management</div>
          {storageSubView === "storage_hub" ? (
            <>
              <p>Choose how you want to manage and delete storage data.</p>
              <div className="settings-hub-grid">
                <button type="button" className="settings-hub-card" onClick={() => setStorageSubView("storage_classic")}>
                  <strong>Classic Management System</strong>
                  <span>Current category/entry view with file and DB target deletion.</span>
                </button>
                <button type="button" className="settings-hub-card" onClick={() => setStorageSubView("storage_per_pair")}>
                  <strong>Per Pair Data</strong>
                  <span>Open each pair, pick available data types, and delete selected.</span>
                </button>
              </div>
            </>
          ) : null}
          {storageSubView === "storage_classic" ? (
            <>
          <p>Manage user-generated storage data with real delete operations.</p>
          <div className="storage-summary-row">
            <div className="storage-summary-card">
              <span>Total Categories</span>
              <strong>{storageOverview?.totals.category_count ?? 0}</strong>
            </div>
            <div className="storage-summary-card">
              <span>Total Entries</span>
              <strong>{storageOverview?.totals.entry_count ?? 0}</strong>
            </div>
            <div className="storage-summary-card">
              <span>Total Size</span>
              <strong>{formatBytes(Number(storageOverview?.totals.size_bytes ?? 0))}</strong>
            </div>
          </div>
          <div className="storage-actions-row">
            <button type="button" onClick={() => void loadStorageOverview()} disabled={storageBusy || storageLoading}>
              {storageLoading ? "Loading..." : "Refresh"}
            </button>
            <button
              type="button"
              className="danger"
              disabled={storageBusy || selectedStorageEntries.length === 0}
              onClick={() => openDeleteModalForEntries(selectedStorageEntries)}
            >
              Delete Selected ({selectedStorageEntries.length})
            </button>
          </div>
          {storageStatus ? <div className="settings-status">{storageStatus}</div> : null}
          {storageError ? <div className="coverage-error">{storageError}</div> : null}
          {storageLoading ? <div className="ml-note">Loading storage data... scanning files and database usage.</div> : null}
          <div className="storage-category-grid">
            {(storageOverview?.categories ?? []).map((category) => (
              <section key={category.category_id} className="storage-category-card">
                <div className="storage-category-head">
                  <div>
                    <strong>{category.label}</strong>
                    <div className="ml-note">
                      {category.entry_count} entries · {formatBytes(Number(category.size_bytes ?? 0))}
                    </div>
                  </div>
                </div>
                <div className="storage-entry-list">
                  {Object.entries(
                    category.entries.reduce<Record<string, StorageEntry[]>>((acc, entry) => {
                      const key = String(entry.pair_symbol || "UNSCOPED").toUpperCase();
                      if (!acc[key]) acc[key] = [];
                      acc[key].push(entry);
                      return acc;
                    }, {})
                  )
                    .sort(([a], [b]) => a.localeCompare(b))
                    .map(([pairKey, entries]) => (
                      <div key={`${category.category_id}-${pairKey}`} className="storage-pair-group">
                        <div className="storage-pair-title">{pairKey}</div>
                        {entries.map((entry) => {
                          const checked = storageSelectedIds.has(entry.entry_id);
                          return (
                            <div key={entry.entry_id} className="storage-entry-row">
                              <label className="storage-check">
                                <input
                                  type="checkbox"
                                  checked={checked}
                                  onChange={(event) => toggleStorageEntry(entry.entry_id, event.target.checked)}
                                />
                              </label>
                              <div className="storage-main">
                                <div className="storage-title">
                                  <span>{entry.name}</span>
                                  <code>{entry.type}</code>
                                  <code>{entry.pair_symbol || "UNSCOPED"}</code>
                                </div>
                                <div className="storage-path">{entry.path}</div>
                                <div className="storage-meta">{formatBytes(Number(entry.size_bytes ?? 0))}</div>
                              </div>
                              <div className="storage-row-actions">
                                <button type="button" onClick={() => void navigator.clipboard.writeText(entry.path)}>
                                  Copy Path
                                </button>
                                <button
                                  type="button"
                                  className="danger"
                                  disabled={storageBusy}
                                  onClick={() => openDeleteModalForEntries([entry])}
                                >
                                  Delete
                                </button>
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    ))}
                  {!category.entries.length ? <div className="ml-note">No entries.</div> : null}
                </div>
              </section>
            ))}
          </div>
            </>
          ) : null}

          {storageSubView === "storage_per_pair" ? (
            <>
              <p>Select a pair card to manage its specific data types.</p>
              {storageStatus ? <div className="settings-status">{storageStatus}</div> : null}
              {storageError ? <div className="coverage-error">{storageError}</div> : null}
              {storageLoading ? <div className="ml-note">Loading storage data... scanning files and database usage.</div> : null}
              <div className="storage-summary-row">
                <div className="storage-summary-card">
                  <span>Pairs With Data</span>
                  <strong>{storagePerPair.length}</strong>
                </div>
                <div className="storage-summary-card">
                  <span>Total Categories</span>
                  <strong>{storageOverview?.totals.category_count ?? 0}</strong>
                </div>
                <div className="storage-summary-card">
                  <span>Total Size</span>
                  <strong>{formatBytes(Number(storageOverview?.totals.size_bytes ?? 0))}</strong>
                </div>
              </div>
              <div className="storage-actions-row">
                <button type="button" onClick={() => void loadStorageOverview()} disabled={storageBusy || storageLoading}>
                  {storageLoading ? "Loading..." : "Refresh"}
                </button>
              </div>
              <div className="storage-pair-card-grid">
                {storagePerPair.map((item) => (
                  <button
                    key={`pair-card-${item.pairSymbol}`}
                    type="button"
                    className="storage-pair-card"
                    onClick={() => openPairStorageDetail(item.pairSymbol)}
                  >
                    <strong>{item.pairSymbol}</strong>
                    <span>{item.kinds.length} data type(s)</span>
                    <span>{formatBytes(item.totalSizeBytes)}</span>
                  </button>
                ))}
              </div>
              {!storagePerPair.length ? <div className="ml-note">No pair-scoped data found.</div> : null}
            </>
          ) : null}

          {storageSubView === "storage_pair_detail" && selectedPairStorage ? (
            <>
              <p>Choose which data of <strong>{selectedPairStorage.pairSymbol}</strong> to delete.</p>
              {storageStatus ? <div className="settings-status">{storageStatus}</div> : null}
              {storageError ? <div className="coverage-error">{storageError}</div> : null}
              <div className="storage-actions-row">
                <button type="button" onClick={() => void loadStorageOverview()} disabled={storageBusy || storageLoading}>
                  {storageLoading ? "Loading..." : "Refresh"}
                </button>
                <button
                  type="button"
                  className="danger"
                  disabled={storageBusy || pairDataSelectedKinds.size === 0}
                  onClick={openDeleteModalForSelectedPairKinds}
                >
                  Delete Selected ({pairDataSelectedKinds.size})
                </button>
              </div>
              <div className="storage-pair-detail-list">
                {selectedPairStorage.kinds.map((kind) => {
                  const checked = pairDataSelectedKinds.has(kind.kind);
                  return (
                    <label key={`kind-${kind.kind}`} className="storage-pair-kind-row">
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={(event) => togglePairDataKind(kind.kind, event.target.checked)}
                      />
                      <div className="storage-pair-kind-main">
                        <strong>{kind.label}</strong>
                        <span>{kind.dbScoped ? "DB scoped" : formatBytes(kind.sizeBytes)}</span>
                      </div>
                    </label>
                  );
                })}
              </div>
            </>
          ) : null}
        </section>
      ) : null}

      {settingsView === "trainer_bot" ? (
      <>
      <section className="panel">
        <div className="settings-section-topbar">
          <button type="button" onClick={() => onSettingsViewChange("hub")}>Back</button>
        </div>
      </section>
      <section className="panel ml-trainer-panel">
        <div className="small-title">Model Trainer &amp; Bot</div>
      </section>
      <section className="panel ml-trainer-panel">
        <div className="ml-instance-strip">
          {instancesLoading ? <div className="ml-note">Loading model/bot cards...</div> : null}
          {!instancesLoading ? <div className="ml-note">Loaded cards: {instances.length}</div> : null}
          {!instancesLoading && instances.length === 0 ? (
            <div className="ml-note">No model/bot cards found yet. Click "+ New Model/Bot" to create one.</div>
          ) : null}
          <div className="ml-instance-cards">
            <button
              type="button"
              className="ml-instance-add"
              onClick={() => void loadInstances()}
              disabled={trainerBusy}
            >
              Refresh
            </button>
            {instances.map((instance) => (
              <div
                key={instance.instance_id}
                role="button"
                tabIndex={0}
                className={`ml-instance-card ${instance.instance_id === selectedInstanceId ? "active" : ""}`}
                onClick={() => {
                  setSelectedInstanceId(instance.instance_id);
                  setMlDetailsOpen(true);
                }}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    setSelectedInstanceId(instance.instance_id);
                    setMlDetailsOpen(true);
                  }
                }}
              >
                <div className="ml-instance-card-head">
                  <div className="ml-instance-title">{instance.name}</div>
                  <button
                    type="button"
                    className="ml-instance-delete"
                    onClick={(event) => {
                      event.stopPropagation();
                      void deleteInstance(instance.instance_id, instance.name);
                    }}
                    disabled={trainerBusy}
                  >
                    Delete
                  </button>
                </div>
                <div className="ml-instance-meta">{instance.pair_symbol}</div>
                <div className="ml-instance-meta">
                  Exchanges: {instance.selected_exchange_count ?? 0} | Bot: {instance.bot_status ?? "IDLE"}
                </div>
              </div>
            ))}
            <button type="button" className="ml-instance-add" onClick={() => void createInstanceFromPrompt()} disabled={trainerBusy}>
              + New Model/Bot
            </button>
          </div>
        </div>

        {!mlDetailsOpen ? (
          <div className="ml-note">Select a bot/model card to open full trainer controls.</div>
        ) : null}

        {mlDetailsOpen ? (
        <div className="ml-subpanel-actions">
          <button type="button" onClick={() => setMlDetailsOpen(false)} disabled={trainerBusy}>
            Back To Bot Cards
          </button>
        </div>
        ) : null}

        {mlDetailsOpen ? (
        <>

        <div className="ml-toolbar">
          <label>
            Pair
            <select
              value={selectedPair}
              onChange={(event) => {
                const nextPair = normalizePair(event.target.value);
                setSelectedPair(nextPair);
                const match = instances.find((it) => normalizePair(it.pair_symbol) === nextPair);
                if (match) setSelectedInstanceId(match.instance_id);
              }}
            >
              {trainerPairs.map((pair) => (
                <option key={pair} value={pair}>
                  {pair}
                </option>
              ))}
              {!trainerPairs.length ? <option value="">No pairs yet</option> : null}
            </select>
          </label>
          <label>
            Add pair
            <div className="ml-inline">
              <input
                value={pairInput}
                onChange={(event) => setPairInput(event.target.value.toUpperCase())}
                placeholder="BTCUSDT"
              />
              <button type="button" onClick={() => void addPair()} disabled={trainerBusy}>
                Add
              </button>
            </div>
          </label>
          <label>
            Run
            <select value={selectedRunId} onChange={(event) => setSelectedRunId(event.target.value)}>
              {runs.map((run) => (
                <option key={run.run_id} value={run.run_id}>
                  {formatRunLabel(run)}
                </option>
              ))}
              {!runs.length ? <option value="">No runs</option> : null}
            </select>
            <button
              type="button"
              className="ml-run-manage-toggle"
              onClick={() => setRunActionsOpen((prev) => !prev)}
              disabled={trainerBusy || !runs.length}
            >
              {runActionsOpen ? "Hide Run Actions" : "Show Run Actions"}
            </button>
            {runActionsOpen ? (
              <div className="ml-run-list">
                {runs.length ? (
                  runs.map((run) => (
                    <div key={run.run_id} className="ml-run-row">
                      <button
                        type="button"
                        className="ml-run-row-delete"
                        onClick={() => void actionDeleteRun(run.run_id)}
                        disabled={trainerBusy || run.status === "RUNNING"}
                      >
                        Delete
                      </button>
                      <button
                        type="button"
                        className={`ml-run-row-select ${run.run_id === selectedRunId ? "active" : ""}`}
                        onClick={() => setSelectedRunId(run.run_id)}
                      >
                        {formatRunLabel(run)}
                      </button>
                    </div>
                  ))
                ) : (
                  <div className="ml-note">No runs</div>
                )}
              </div>
            ) : null}
          </label>
          <div className="ml-actions">
            <button
              type="button"
              onClick={() => void actionStartRun()}
              disabled={trainerBusy || !selectedPair || hasQueuedOrRunningRun || (workloadControls ? !workloadControls.training_enabled : false)}
            >
              Start
            </button>
            <button type="button" onClick={() => void actionPauseRun()} disabled={trainerBusy || !selectedRunId || !canPauseOrStopSelectedRun}>
              Pause
            </button>
            <button type="button" onClick={() => void actionStopRun()} disabled={trainerBusy || !selectedRunId || !canPauseOrStopSelectedRun}>
              Stop
            </button>
            <button
              type="button"
              onClick={() => void actionApproveRun()}
              disabled={
                trainerBusy ||
                !selectedRunSummary ||
                selectedRunSummary.status !== "COMPLETED" ||
                !approvalGateStatus.canApprove
              }
              title={!approvalGateStatus.canApprove ? `Rejected: ${approvalGateStatus.blockedReason}` : undefined}
            >
              Approve
            </button>
            <button type="button" onClick={() => void renameSelectedInstance()} disabled={trainerBusy || !selectedInstanceId}>
              Rename Bot
            </button>
          </div>
          <label style={{ display: "grid", gap: 4, marginTop: 6, maxWidth: 360 }}>
            Dataset Source
            <select
              value={trainingDatasetSource}
              onChange={(event) => {
                const nextSource: TrainingDatasetSource =
                  event.target.value === "local_market_events" ? "local_market_events" : "features_manifest";
                setTrainingDatasetSource(nextSource);
                if (selectedPair) {
                  void loadDataStatus(selectedPair, selectedInstanceId || undefined, nextSource);
                }
              }}
              disabled={trainerBusy}
            >
              <option value="features_manifest">Feature manifests (current)</option>
              <option value="local_market_events">Local market events (full fidelity)</option>
            </select>
          </label>
          {runActionProgress ? (
            <div className="ml-backfill-progress-wrap ml-run-action-progress">
              <div className="ml-backfill-progress-labels">
                <span>{runActionTitle}</span>
                <span>{runActionProgressPct}%</span>
              </div>
              <div className="ml-backfill-progress-track">
                <div className="ml-backfill-progress-fill ml-run-action-progress-fill" style={{ width: `${runActionProgressPct}%` }} />
              </div>
              <div className="ml-backfill-progress-meta">
                <span>Elapsed: {formatDurationMs(runActionElapsedMs)}</span>
                <span>ETA: {runActionProgress.state === "done" ? "Done" : formatDurationMs(runActionEtaMs)}</span>
              </div>
            </div>
          ) : null}
        </div>

        <div className="ml-stage-row">
          {stageOrder.map((stage) => (
            <div key={stage} className={`ml-stage ${stageState(selectedRunSummary, stage)}`}>
              <span>{stage.toUpperCase()}</span>
            </div>
          ))}
        </div>

        <div className="ml-kpi-grid">
          <div className="ml-kpi-card">
            <span>Status</span>
            <strong>{selectedRunSummary?.status ?? "IDLE"}</strong>
          </div>
          <div className="ml-kpi-card">
            <span>Stage</span>
            <strong>{selectedRunSummary?.stage ?? "-"}</strong>
          </div>
          <div className="ml-kpi-card">
            <span>Progress</span>
            <strong>{selectedRunSummary ? `${Math.round((selectedRunSummary.stage_progress ?? 0) * 100)}%` : "-"}</strong>
          </div>
          <div className="ml-kpi-card">
            <span>Device</span>
            <strong>{selectedRunSummary?.device ?? "-"}</strong>
          </div>
          <div className="ml-kpi-card">
            <span>Data hours (selected)</span>
            <strong>
              {dataStatus ? `${selectedDataHours.toFixed(2)}` : "-"}
            </strong>
          </div>
          <div className="ml-kpi-card">
            <span>Rows (selected)</span>
            <strong>
              {dataStatus ? `${selectedRows.toLocaleString()}` : "-"}
            </strong>
          </div>
          <div className="ml-kpi-card">
            <span>Used Data (GB)</span>
            <strong>
              {(() => {
                const used = telemetryNumber("used_data_gb");
                const total = telemetryNumber("total_data_gb");
                if (used == null || total == null) return "-";
                return `${used.toFixed(2)} / ${total.toFixed(2)}`;
              })()}
            </strong>
          </div>
          <div className="ml-kpi-card">
            <span>Loaded Now (GB)</span>
            <strong>{(telemetryNumber("process_rss_gb") ?? telemetryNumber("loaded_now_gb"))?.toFixed(3) ?? "-"}</strong>
          </div>
          <div className="ml-kpi-card">
            <span>RAM Budget (GB)</span>
            <strong>{telemetryNumber("ram_budget_gb")?.toFixed(2) ?? "-"}</strong>
            <small style={{ color: "var(--muted)" }}>
              Current setting:{" "}
              {workloadControls?.training_ram_budget_gb == null
                ? "Auto"
                : Number(workloadControls.training_ram_budget_gb).toFixed(2)}
            </small>
          </div>
          <div className="ml-kpi-card">
            <span>Chunks</span>
            <strong>
              {(() => {
                const processed = telemetryNumber("data_chunks_processed") ?? telemetryNumber("chunks_processed");
                const total = telemetryNumber("data_chunks_total") ?? telemetryNumber("chunks_total");
                if (processed == null || total == null) return "-";
                return `${Math.round(processed)} / ${Math.round(total)}`;
              })()}
            </strong>
          </div>
          <div className="ml-kpi-card">
            <span>Windows</span>
            <strong>
              {(() => {
                const processed = telemetryNumber("windows_processed");
                const total = telemetryNumber("windows_total");
                if (processed == null || total == null) return "-";
                return `${Math.round(processed).toLocaleString()} / ${Math.round(total).toLocaleString()}`;
              })()}
            </strong>
          </div>
          <div className="ml-kpi-card">
            <span>Checkpoint</span>
            <strong>{String(selectedRunMetrics["checkpoint_step"] ?? "-")}</strong>
          </div>
        </div>

        <div className="ml-grid">
          <section className="panel ml-subpanel">
            <div className="small-title">Pair Profile</div>
            {profileDraft ? (
              <div className="ml-profile-summary">
                <div className="kv"><span>Pair</span><span>{profileDraft.pair_symbol || "-"}</span></div>
                <div className="kv"><span>Training exchanges</span><span>{profileDraft.training_selected_exchanges.length}</span></div>
                <div className="kv"><span>Bot data exchanges</span><span>{profileDraft.bot_data_selected_exchanges.length}</span></div>
                <div className="kv"><span>Bot exec exchanges</span><span>{profileDraft.bot_execution_selected_exchanges.length}</span></div>
                <div className="kv"><span>Use local data</span><span>{profileDraft.use_local_data ? "Yes" : "No"}</span></div>
                <div className="kv"><span>Use historic data</span><span>{profileDraft.use_historic_data ? "Yes" : "No"}</span></div>
                <div className="kv"><span>Historic days</span><span>{profileDraft.historic_data_days}</span></div>
                <div className="kv"><span>Historic max days</span><span>{profileDraft.historic_data_days_max ?? 3650}</span></div>
                <div className="kv"><span>Horizons</span><span>{profileDraft.horizons.join(", ")}</span></div>
                <div className="kv"><span>Threshold</span><span>{profileDraft.label_threshold_pct}</span></div>
                <div className="kv"><span>Min data hours</span><span>{profileDraft.min_data_hours}</span></div>
                <div className="kv"><span>Hour window</span><span>{profileDraft.training_hour_window_enabled ? "On" : "Off"}</span></div>
                <div className="kv">
                  <span>Window range</span>
                  <span>{`${profileDraft.training_hour_start}..${profileDraft.training_hour_end} (0=endpoints)`}</span>
                </div>
                <div className="kv"><span>Updated</span><span>{profileDraft.updated_at ? new Date(profileDraft.updated_at).toLocaleString() : "-"}</span></div>
              </div>
            ) : (
              <div className="ml-note">Select a pair to edit profile.</div>
            )}
            <div className="ml-subpanel-actions">
              <button type="button" onClick={openProfileModal} disabled={trainerBusy || !selectedPair}>
                Open Pair Profile
              </button>
              <button type="button" onClick={openBotSettingsModal} disabled={trainerBusy || !selectedPair}>
                Open Bot Settings
              </button>
              <button type="button" onClick={openReplayTraceSettingsModal} disabled={trainerBusy || !selectedPair}>
                Open Replay Trace Settings
              </button>
              <button type="button" onClick={openExpSettingsModal} disabled={trainerBusy || !selectedPair}>
                Open Exp Settings
              </button>
              <button
                type="button"
                onClick={() => {
                  if (selectedPair) void loadProfile(selectedPair, selectedInstanceId || undefined);
                }}
                disabled={trainerBusy || !selectedPair}
              >
                Reset Draft
              </button>
              <span className={`ml-dirty ${profileDirty ? "dirty" : ""}`}>{profileDirty ? "Unsaved profile changes" : "Profile saved"}</span>
            </div>
            <div className="ml-historic-block">
              <div className="small-title">Historic Data</div>
              <div className="ml-historic-actions">
                <button
                  type="button"
                  onClick={() => void actionBackfillStart()}
                  disabled={trainerBusy || !selectedPair || (workloadControls ? !workloadControls.backfill_enabled : false)}
                >
                  Acquire Historic Data
                </button>
                <button
                  type="button"
                  onClick={() => void actionBackfillPause()}
                  disabled={trainerBusy || !backfillStatus || backfillStatus.status !== "RUNNING"}
                >
                  Pause
                </button>
                <button
                  type="button"
                  onClick={() => void actionBackfillStop()}
                  disabled={trainerBusy || !backfillStatus || !["RUNNING", "PAUSED"].includes(backfillStatus.status)}
                >
                  Stop
                </button>
                <button
                  type="button"
                  onClick={() => void actionBackfillClear()}
                  disabled={trainerBusy || !selectedPair}
                  style={{ borderColor: "#9e3b3b", color: "#ff8f8f" }}
                >
                  Delete Historic Data
                </button>
              </div>
              <div className="ml-profile-summary">
                <div className="kv"><span>Active source</span><span>{dataStatus?.active_source ?? "-"}</span></div>
                <div className="kv"><span>Source path</span><span>{dataStatus?.active_source_path ?? "-"}</span></div>
                <div className="kv"><span>Backfill mode</span><span>{backfillStatus?.mode ?? dataStatus?.mode ?? "-"}</span></div>
                <div className="kv"><span>Selected exchanges</span><span>{(backfillStatus?.selected_exchanges ?? dataStatus?.selected_exchanges ?? []).length}</span></div>
                <div className="kv"><span>Data hours (selected)</span><span>{dataStatus ? selectedDataHours.toFixed(2) : "-"}</span></div>
                <div className="kv"><span>Rows (selected)</span><span>{dataStatus ? selectedRows.toLocaleString() : "-"}</span></div>
              </div>
              {backfillStatus ? (
                <div className="ml-profile-summary">
                  <div className="ml-backfill-progress-wrap">
                    <div className="ml-backfill-progress-labels">
                      <span>{backfillStatus.status}</span>
                      <span>{backfillProgressPct}%</span>
                    </div>
                    <div className="ml-backfill-progress-track" aria-label="Historic data progress">
                      <div className="ml-backfill-progress-fill" style={{ width: `${backfillProgressPct}%` }} />
                    </div>
                    <div className="ml-backfill-progress-meta">
                      <span>Elapsed: {formatDurationMs(backfillElapsedMs)}</span>
                      <span>ETA: {backfillEtaMs ? formatDurationMs(backfillEtaMs) : "-"}</span>
                    </div>
                  </div>
                  <div className="kv"><span>Job</span><span>{backfillStatus.job_id}</span></div>
                  <div className="kv"><span>Status</span><span>{backfillStatus.status}</span></div>
                  <div className="kv"><span>Stage</span><span>{backfillStatus.stage}</span></div>
                  <div className="kv"><span>Progress</span><span>{Math.round((backfillStatus.progress ?? 0) * 100)}%</span></div>
                  <div className="kv"><span>Target path</span><span>{backfillStatus.target_path}</span></div>
                  {backfillStatus.error_text ? <div className="ml-error-text">{backfillStatus.error_text}</div> : null}
                </div>
              ) : (
                <div className="ml-note">No historic acquisition job for this pair yet.</div>
              )}
              {(backfillStatus?.exchange_progress?.length ?? 0) > 0 ? (
                <div className="ml-profile-summary">
                  <div className="small-title">Exchange Coverage</div>
                  <div className="ml-exchange-progress-list">
                    {backfillStatus?.exchange_progress?.map((item) => (
                      <div key={`${item.exchange_id}-${item.status}`} className="ml-exchange-progress-row">
                        <div className="left">{item.exchange_name || item.exchange_id.toUpperCase()}</div>
                        <div>{item.status}</div>
                        <div>{Number(item.data_hours || 0).toFixed(2)}h</div>
                        <div>{Number(item.rows || 0).toLocaleString()} rows</div>
                        <div className="reason">{item.reason || "-"}</div>
                      </div>
                    ))}
                  </div>
                </div>
              ) : null}
              <div className="ml-log-box ml-log-box-compact">
                {backfillLogs.length ? (
                  backfillLogs.map((log, index) => (
                    <div key={`${log.ts}-${index}`} className={`ml-log-line ${log.level.toLowerCase()}`}>
                      <span>{new Date(log.ts).toLocaleTimeString()}</span>
                      <span>[{log.level}]</span>
                      <span>{log.message}</span>
                    </div>
                  ))
                ) : (
                  <div className="ml-note">No historic-data logs yet.</div>
                )}
              </div>
            </div>
          </section>

          <section className="panel ml-subpanel ml-run-logs-panel">
            <div className="ml-subpanel-head">
              <div className="small-title">Run Logs</div>
              <div className="ml-inline" style={{ gap: 6 }}>
                <button type="button" onClick={() => clearRunLogsPanel()} disabled={!runLogs.length}>
                  Clear Logs
                </button>
                <button type="button" onClick={() => void copyRunLogsToClipboard()} disabled={!runLogs.length}>
                  Copy Logs
                </button>
              </div>
            </div>
            <div className="ml-log-box ml-log-box-run">
              {runLogs.length ? (
                runLogs.map((log, index) => (
                  <div key={`${log.ts}-${index}`} className={`ml-log-line ${log.level.toLowerCase()}`}>
                    <span>{new Date(log.ts).toLocaleTimeString()}</span>
                    <span>[{log.level}]</span>
                    <span>{log.message}</span>
                  </div>
                ))
              ) : (
                <div className="ml-note">No logs yet.</div>
              )}
            </div>
          </section>
        </div>

        <section className="panel ml-subpanel">
          <div className="ml-subpanel-head">
            <div className="small-title">Model Performance</div>
            <div className="ml-inline" style={{ gap: 6 }}>
              <button
                type="button"
                onClick={() => void actionApplyRecommendedGates()}
                disabled={trainerBusy || !selectedRunSummary || !hasRecommendedGates}
                title="Apply recommended confidence/quality/gap thresholds from this run to the pair profile"
              >
                Apply Recommended Gates
              </button>
            </div>
          </div>
          {selectedRunSummary ? (
            <div className="ml-metrics-table-wrap">
              {(() => {
                const metrics = (selectedRunSummary.metrics ?? {}) as Record<string, unknown>;
                const asNum = (key: string, fallback = 0) => Number(metrics[key] ?? fallback);
                const wfReasonsRaw = Array.isArray(metrics.walk_forward_rejection_reasons)
                  ? (metrics.walk_forward_rejection_reasons as Array<unknown>).map((v) => String(v)).filter(Boolean)
                  : [];
                const confusionRaw = metrics["direction_confusion_matrix"];
                const classOrderRaw = metrics["direction_class_order"];
                const confusion =
                  Array.isArray(confusionRaw) &&
                  confusionRaw.every((row) => Array.isArray(row))
                    ? (confusionRaw as unknown[][])
                    : null;
                const classOrder =
                  Array.isArray(classOrderRaw) && classOrderRaw.length === 3
                    ? (classOrderRaw.map((item) => String(item)) as string[])
                    : ["down", "flat", "up"];
                return (
                  <>
              {!Boolean(metrics.approval_ok) ? (
                <div className="ml-note" style={{ marginBottom: 10, border: "1px solid #ff616155", padding: "8px 10px" }}>
                  <strong>Why rejected:</strong>{" "}
                  {String(metrics.approval_reason ?? "validation_overfit_or_test_failed")}
                  <div style={{ marginTop: 4 }}>
                    recommended_action=NO_TRADE | deploy_allowed={Boolean(metrics.deploy_allowed) ? "true" : "false"} | final_test_threshold=
                    {Number(metrics.test_gated_confidence_threshold_used ?? metrics.recommended_confidence_threshold ?? 0).toFixed(4)}
                  </div>
                  <div style={{ marginTop: 4 }}>
                    false_long_rate={Number(metrics.false_long_rate ?? 0).toFixed(4)} | false_short_rate={Number(metrics.false_short_rate ?? 0).toFixed(4)} | long_precision=
                    {Number(metrics.long_precision ?? 0).toFixed(4)} | short_precision={Number(metrics.short_precision ?? 0).toFixed(4)}
                  </div>
                  <div style={{ marginTop: 4 }}>
                    final_test_pnl_after_cost={Number(metrics.test_gated_pnl_proxy_after_cost ?? 0).toFixed(6)} | final_test_action_rate=
                    {Number(metrics.test_gated_action_rate ?? 0).toFixed(6)}
                  </div>
                  {wfReasonsRaw.length ? (
                    <div style={{ marginTop: 4 }}>walk_forward_reasons={wfReasonsRaw.join(", ")}</div>
                  ) : null}
                </div>
              ) : null}
              <table className="ml-metrics-table">
                <thead>
                  <tr>
                    <th>Metric</th>
                    <th>Value</th>
                  </tr>
                </thead>
                <tbody>
                  <tr>
                    <td>direction_accuracy</td>
                    <td>{asNum("direction_accuracy").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>direction_precision_macro</td>
                    <td>{asNum("direction_precision_macro").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>direction_recall_macro</td>
                    <td>{asNum("direction_recall_macro").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>direction_f1_macro</td>
                    <td>{asNum("direction_f1_macro").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>direction_precision_down</td>
                    <td>{asNum("direction_precision_down").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>direction_precision_flat</td>
                    <td>{asNum("direction_precision_flat").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>direction_precision_up</td>
                    <td>{asNum("direction_precision_up").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>direction_recall_down</td>
                    <td>{asNum("direction_recall_down").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>direction_recall_flat</td>
                    <td>{asNum("direction_recall_flat").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>direction_recall_up</td>
                    <td>{asNum("direction_recall_up").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>direction_f1_down</td>
                    <td>{asNum("direction_f1_down").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>direction_f1_flat</td>
                    <td>{asNum("direction_f1_flat").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>direction_f1_up</td>
                    <td>{asNum("direction_f1_up").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>magnitude_rmse</td>
                    <td>{asNum("magnitude_rmse").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>quality_accuracy</td>
                    <td>{asNum("quality_accuracy").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>pnl_proxy</td>
                    <td>{asNum("pnl_proxy").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>sharpe_proxy</td>
                    <td>{asNum("sharpe_proxy").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>best_val_loss</td>
                    <td>{asNum("best_val_loss").toFixed(6)}</td>
                  </tr>
                  <tr>
                    <td>samples_test</td>
                    <td>{Math.round(asNum("samples_test")).toLocaleString()}</td>
                  </tr>
                </tbody>
              </table>
              {confusion ? (
                <>
                  <div className="small-title">Direction Confusion Matrix</div>
                  <table className="ml-metrics-table">
                    <thead>
                      <tr>
                        <th>True \ Pred</th>
                        <th>{classOrder[0]}</th>
                        <th>{classOrder[1]}</th>
                        <th>{classOrder[2]}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {confusion.map((row, rowIndex) => (
                        <tr key={`cm-${rowIndex}`}>
                          <td>{classOrder[rowIndex] ?? `class_${rowIndex}`}</td>
                          {(row as unknown[]).slice(0, 3).map((value, colIndex) => (
                            <td key={`cm-${rowIndex}-${colIndex}`}>{Math.round(Number(value ?? 0)).toLocaleString()}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </>
              ) : null}
                  </>
                );
              })()}
            </div>
          ) : (
            <div className="ml-note">No run selected.</div>
          )}
        </section>

        <section className="panel ml-subpanel">
          <div className="small-title">Paper Bot</div>
          <div className="ml-bot-actions">
            <button
              type="button"
              onClick={() => void actionBotStart()}
              disabled={trainerBusy || !selectedPair || botStatus?.status === "RUNNING" || (workloadControls ? !workloadControls.bots_enabled : false)}
            >
              Start Bot
            </button>
            <button
              type="button"
              onClick={() => void actionBotPause()}
              disabled={trainerBusy || !selectedPair || botStatus?.status !== "RUNNING"}
            >
              Pause Bot
            </button>
            <button
              type="button"
              onClick={() => void actionBotStop()}
              disabled={trainerBusy || !selectedPair || (botStatus?.status !== "RUNNING" && botStatus?.status !== "PAUSED")}
            >
              Stop Bot
            </button>
          </div>
          {botStatus ? (
            <>
              {(() => {
                const initialBalance = Number(botStatus.initial_balance ?? 100);
                const balance = Number(botStatus.current_balance ?? (initialBalance + Number(botStatus.realized_pnl || 0)));
                const equity = Number(botStatus.equity ?? (balance + Number(botStatus.unrealized_pnl || 0)));
                const balanceColor = balance >= initialBalance ? "#23d18b" : "#ff6161";
                const equityColor = equity >= initialBalance ? "#23d18b" : "#ff6161";
                const healthSource = String(botStatus.bot_data_source || "unknown");
                const healthFreshness = botStatus.snapshot_freshness_sec;
                const freshnessLabel =
                  healthFreshness == null || !Number.isFinite(Number(healthFreshness))
                    ? "n/a"
                    : `${Number(healthFreshness).toFixed(1)}s`;
                const activeExchanges = Number(botStatus.active_exchange_count ?? 0);
                return (
                  <>
                    <div className="kv"><span>Initial Balance</span><span>{initialBalance.toFixed(4)}</span></div>
                    <div className="kv"><span>Balance</span><span style={{ color: balanceColor }}>{balance.toFixed(4)}</span></div>
                    <div className="kv"><span>Equity</span><span style={{ color: equityColor }}>{equity.toFixed(4)}</span></div>
                    <div className="kv"><span>Bot Data Health</span><span>{`source=${healthSource} | freshness=${freshnessLabel} | exchanges=${activeExchanges}`}</span></div>
                  </>
                );
              })()}
              <div className="kv"><span>Status</span><span>{botStatus.status}</span></div>
              <div className="kv"><span>Position</span><span>{botStatus.position_side || "-"}</span></div>
              <div className="kv"><span>Entry</span><span>{botStatus.entry_price.toFixed(6)}</span></div>
              <div className="kv"><span>Qty</span><span>{botStatus.qty.toFixed(4)}</span></div>
              <div className="kv"><span>Unrealized</span><span>{botStatus.unrealized_pnl.toFixed(4)}</span></div>
              <div className="kv"><span>Realized</span><span>{botStatus.realized_pnl.toFixed(4)}</span></div>
              {(() => {
                const lastSignal = botStatus.last_signal ?? {};
                const realizedByExchangeRaw = (lastSignal as Record<string, unknown>).realized_by_exchange;
                const openPositionsRaw = (lastSignal as Record<string, unknown>).open_positions_by_exchange;
                const realizedByExchange =
                  realizedByExchangeRaw && typeof realizedByExchangeRaw === "object"
                    ? (realizedByExchangeRaw as Record<string, unknown>)
                    : {};
                const openPositions =
                  openPositionsRaw && typeof openPositionsRaw === "object"
                    ? (openPositionsRaw as Record<string, unknown>)
                    : {};
                const exchangeIds = Array.from(new Set([...Object.keys(realizedByExchange), ...Object.keys(openPositions)]));
                if (!exchangeIds.length) return null;
                return (
                  <div className="ml-table-wrap" style={{ marginTop: 8 }}>
                    <div className="small-title" style={{ marginBottom: 6 }}>Execution Exchanges</div>
                    <table className="ml-table">
                      <thead>
                        <tr>
                          <th>Exchange</th>
                          <th>Side</th>
                          <th>Entry</th>
                          <th>Qty</th>
                          <th>Realized</th>
                        </tr>
                      </thead>
                      <tbody>
                        {exchangeIds.map((exchangeId) => {
                          const pos = (openPositions[exchangeId] as Record<string, unknown> | undefined) ?? {};
                          const side = String(pos.side ?? "-");
                          const entry = Number(pos.entry_price ?? 0);
                          const qty = Number(pos.qty ?? 0);
                          const realized = Number(realizedByExchange[exchangeId] ?? 0);
                          return (
                            <tr key={`bot-ex-${exchangeId}`}>
                              <td>{exchangeId}</td>
                              <td>{side || "-"}</td>
                              <td>{entry ? entry.toFixed(6) : "-"}</td>
                              <td>{qty ? qty.toFixed(4) : "-"}</td>
                              <td>{realized.toFixed(6)}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                );
              })()}
              <pre>{JSON.stringify(botStatus.last_signal ?? {}, null, 2)}</pre>
            </>
          ) : (
            <div className="ml-note">No bot state yet.</div>
          )}
        </section>
        <section className="panel ml-subpanel">
          <div className="ml-subpanel-head">
            <div className="ml-inline" style={{ gap: 8, flexWrap: "nowrap", alignItems: "center" }}>
              <div className="small-title">Bot Trade History</div>
              {selectedInstanceId ? (
                <>
                  <select
                    value={activeBotSessionId}
                    onChange={(event) => void setActiveBotSession(event.target.value)}
                    style={{ minWidth: 170 }}
                  >
                    {botSessions.map((session) => (
                      <option key={session.session_id} value={session.session_id}>
                        {session.name} ({session.trade_count})
                      </option>
                    ))}
                  </select>
                  <div className="ml-inline" style={{ gap: 6, flexWrap: "nowrap" }}>
                    <button
                      type="button"
                      style={{ padding: "4px 10px", minHeight: 30, fontSize: 12 }}
                      onClick={() => {
                        setBotSessionCreateOpen((prev) => !prev);
                        setBotSessionRenameOpen(false);
                        setBotSessionInput("");
                      }}
                    >
                      +
                    </button>
                    <button
                      type="button"
                      style={{ padding: "4px 10px", minHeight: 30, fontSize: 12 }}
                      disabled={!selectedBotSession}
                      onClick={() => {
                        if (!selectedBotSession) return;
                        setBotSessionRenameOpen((prev) => !prev);
                        setBotSessionCreateOpen(false);
                        setBotSessionInput(selectedBotSession.name);
                      }}
                    >
                      Rename
                    </button>
                    <button
                      type="button"
                      style={{ padding: "4px 10px", minHeight: 30, fontSize: 12 }}
                      disabled={!selectedBotSession || selectedBotSession.is_default}
                      onClick={() => void deleteBotSession()}
                    >
                      Delete
                    </button>
                    <button
                      type="button"
                      style={{ padding: "4px 10px", minHeight: 30, fontSize: 12 }}
                      disabled={!selectedBotSession}
                      onClick={() => void clearBotSessionHistory()}
                    >
                      Clear
                    </button>
                  </div>
                </>
              ) : null}
            </div>
            <div className="ml-inline" style={{ position: "relative" }}>
              <button
                type="button"
                onClick={() => setBotExportMenuOpen((prev) => !prev)}
                disabled={!botTrades.length && totalBotSessionTrades <= 0}
              >
                Export
              </button>
              {botExportMenuOpen && (botTrades.length > 0 || totalBotSessionTrades > 0) ? (
                <div className="panel" style={{ position: "absolute", top: "110%", right: 0, zIndex: 20, minWidth: 150 }}>
                  <div className="ml-inline" style={{ display: "grid", gap: 8 }}>
                    <select
                      value={botExportScope}
                      onChange={(event) => setBotExportScope(event.target.value === "all" ? "all" : "selected")}
                    >
                      <option value="selected">Selected Session</option>
                      <option value="all">All Sessions</option>
                    </select>
                    <button type="button" onClick={() => void exportBotTrades("text", botExportScope)}>
                      Text File
                    </button>
                    <button type="button" onClick={() => void exportBotTrades("doc", botExportScope)}>
                      Word File
                    </button>
                    <button type="button" onClick={() => void exportBotTrades("pdf", botExportScope)}>
                      PDF File
                    </button>
                    <button type="button" onClick={() => void exportBotTrades("csv", botExportScope)}>
                      Excel File
                    </button>
                  </div>
                </div>
              ) : null}
            </div>
          </div>
          {botSessionCreateOpen ? (
            <div className="ml-inline" style={{ gap: 8, marginBottom: 8 }}>
              <input
                value={botSessionInput}
                onChange={(event) => setBotSessionInput(event.target.value)}
                placeholder="New session name"
                style={{ maxWidth: 220 }}
              />
              <button type="button" onClick={() => void createBotSession()}>
                Create
              </button>
              <button type="button" onClick={() => setBotSessionCreateOpen(false)}>
                Cancel
              </button>
            </div>
          ) : null}
          {botSessionRenameOpen ? (
            <div className="ml-inline" style={{ gap: 8, marginBottom: 8 }}>
              <input
                value={botSessionInput}
                onChange={(event) => setBotSessionInput(event.target.value)}
                placeholder="Rename session"
                style={{ maxWidth: 220 }}
              />
              <button type="button" onClick={() => void renameBotSession()}>
                Save Name
              </button>
              <button type="button" onClick={() => setBotSessionRenameOpen(false)}>
                Cancel
              </button>
            </div>
          ) : null}
          {botTrades.length ? (
            <div className="ml-metrics-table-wrap">
              <table className="ml-metrics-table">
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Side</th>
                    <th>Qty</th>
                    <th>Lev</th>
                    <th>Entry</th>
                    <th>Exit</th>
                    <th>Fees</th>
                    <th>Net PnL</th>
                    <th>Reason</th>
                    <th>Open Reason</th>
                  </tr>
                </thead>
                <tbody>
                  {botTrades.map((trade) => (
                    <tr key={trade.trade_id}>
                      {(() => {
                        const ctx = (trade.decision_context ?? {}) as Record<string, unknown>;
                        const num = (key: string, fallback = Number.NaN) => {
                          const v = Number(ctx[key]);
                          return Number.isFinite(v) ? v : fallback;
                        };
                        const fmt = (value: number, digits: number) => (Number.isFinite(value) ? value.toFixed(digits) : "n/a");
                        const txt = (key: string, fallback = "-") => {
                          const v = ctx[key];
                          return v === undefined || v === null || v === "" ? fallback : String(v);
                        };
                        const openReason = txt("entry_reason", "model_inference");
                        const conf = num("entry_confidence");
                        const qual = num("entry_quality");
                        const gap = num("entry_confidence_gap");
                        const spreadPct = num("entry_spread_pct");
                        const speed = num("entry_trade_rate_10s");
                        const buy = num("entry_ladder_buy_pct");
                        const sell = num("entry_ladder_sell_pct");
                        const liq60 = num("entry_liq_events_60s");
                        const details = `open=${openReason} | conf=${fmt(conf, 3)} qual=${fmt(qual, 3)} gap=${fmt(gap, 3)} | spread=${fmt(
                          spreadPct,
                          6
                        )}% speed=${fmt(speed, 3)} | liq buy/sell=${fmt(buy, 2)}/${fmt(sell, 2)} liq60=${fmt(liq60, 0)}`;
                        return (
                          <>
                      <td>{new Date(trade.closed_at).toLocaleString()}</td>
                      <td>{trade.side}</td>
                      <td>{trade.qty.toFixed(4)}</td>
                      <td>{trade.leverage.toFixed(2)}x</td>
                      <td>{trade.entry_price.toFixed(6)}</td>
                      <td>{trade.exit_price.toFixed(6)}</td>
                      <td>{(trade.entry_fee + trade.exit_fee).toFixed(6)}</td>
                      <td>{trade.net_pnl.toFixed(6)}</td>
                      <td>{trade.close_reason}</td>
                            <td
                              title={details}
                              style={{
                                maxWidth: 520,
                                whiteSpace: "normal",
                                overflow: "visible",
                                textOverflow: "clip",
                                wordBreak: "break-word",
                                lineHeight: 1.25,
                              }}
                            >
                              {details}
                            </td>
                          </>
                        );
                      })()}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="ml-note">No closed trades yet.</div>
          )}
        </section>
        {selectedInstanceId ? (
          <BotTradeMap
            candles={botMapCandles}
            trades={botTrades}
            inferencePoints={botMapInferencePoints}
            loading={botMapLoading}
            error={botMapError}
            onRefresh={() => void loadBotTradeMapCandles(selectedInstanceId)}
            confidenceThreshold={profileDraft?.paper_bot?.confidence_threshold ?? 0.72}
            qualityThreshold={profileDraft?.paper_bot?.quality_threshold ?? 0.72}
            liveConfidence={Number((botStatus?.last_signal ?? {})["confidence"] ?? NaN)}
            liveQuality={Number((botStatus?.last_signal ?? {})["quality"] ?? NaN)}
          />
        ) : null}
        </>
        ) : null}

        {trainerStatus ? <div className="settings-status">{trainerStatus}</div> : null}
        {trainerError ? <div className="coverage-error">{trainerError}</div> : null}
      </section>
      </>
      ) : null}

      {settingsView === "trainer_bot" && profileModalOpen ? (
        <div className="coverage-modal-backdrop" onClick={onProfileBackdropClick}>
          <section className="panel coverage-modal ml-profile-modal" onClick={(event) => event.stopPropagation()}>
            <div className="small-title">
              {profileModalSection === "bot"
                ? "Bot Settings"
                : profileModalSection === "trace"
                  ? "Replay Trace Settings"
                  : profileModalSection === "exp"
                    ? "Exp Settings"
                  : "Pair Profile"}{" "}
              - {selectedPair || "-"}
            </div>
            <div className="ml-subpanel-actions ml-profile-modal-actions">
              <button type="button" onClick={exportProfileDraft} disabled={trainerBusy || !profileDraft || !selectedPair}>
                Export Profile
              </button>
              <button type="button" onClick={openImportProfilePicker} disabled={trainerBusy || !selectedPair}>
                Import Profile
              </button>
              <input
                ref={profileImportInputRef}
                type="file"
                accept=".json,application/json"
                style={{ display: "none" }}
                onChange={handleImportProfileFile}
              />
              {profileModalSection !== "trace" ? (
                renderProfileModalActions()
              ) : null}
              {profileModalSection === "trace" ? (
                <span className={`ml-dirty ${profileDirty ? "dirty" : ""}`}>{profileDirty ? "Unsaved profile changes" : "Profile saved"}</span>
              ) : null}
            </div>
            {profileDraft ? (
              <div className="ml-form-grid">
                {profileModalSection === "pair" ? (
                  <label>
                    Training exchanges
                    <div className="ml-inline" style={{ gap: 6, marginBottom: 6 }}>
                      <button
                        type="button"
                        style={{ padding: "4px 10px", minHeight: 28, fontSize: 12 }}
                        onClick={() => setExchangeListAll("training_selected_exchanges", true)}
                      >
                        Check all
                      </button>
                      <button
                        type="button"
                        style={{ padding: "4px 10px", minHeight: 28, fontSize: 12 }}
                        onClick={() => setExchangeListAll("training_selected_exchanges", false)}
                      >
                        Uncheck all
                      </button>
                    </div>
                    <div className="ml-exchange-grid">
                      {allExchangeOptions.map((option) => (
                        <label key={option.exchange_id} className="ml-exchange-item">
                          <input
                            type="checkbox"
                            checked={profileDraft.training_selected_exchanges.includes(option.exchange_id)}
                            onChange={(event) => toggleExchangeList("training_selected_exchanges", option.exchange_id, event.target.checked)}
                          />
                          <span>{option.name}</span>
                        </label>
                      ))}
                      {!allExchangeOptions.length ? <div className="ml-note">No exchange catalog loaded yet.</div> : null}
                    </div>
                  </label>
                ) : profileModalSection === "bot" ? (
                  <>
                    <label>
                      Bot data exchanges
                      <div className="ml-inline" style={{ gap: 6, marginBottom: 6 }}>
                        <button
                          type="button"
                          style={{ padding: "4px 10px", minHeight: 28, fontSize: 12 }}
                          onClick={() => setExchangeListAll("bot_data_selected_exchanges", true)}
                        >
                          Check all
                        </button>
                        <button
                          type="button"
                          style={{ padding: "4px 10px", minHeight: 28, fontSize: 12 }}
                          onClick={() => setExchangeListAll("bot_data_selected_exchanges", false)}
                        >
                          Uncheck all
                        </button>
                      </div>
                      <div className="ml-exchange-grid">
                        {allExchangeOptions.map((option) => (
                          <label key={`botdata-${option.exchange_id}`} className="ml-exchange-item">
                            <input
                              type="checkbox"
                              checked={profileDraft.bot_data_selected_exchanges.includes(option.exchange_id)}
                              onChange={(event) => toggleExchangeList("bot_data_selected_exchanges", option.exchange_id, event.target.checked)}
                            />
                            <span>{option.name}</span>
                          </label>
                        ))}
                      </div>
                    </label>
                    <label>
                      Bot execution exchanges
                      <div className="ml-inline" style={{ gap: 6, marginBottom: 6 }}>
                        <button
                          type="button"
                          style={{ padding: "4px 10px", minHeight: 28, fontSize: 12 }}
                          onClick={() => setExchangeListAll("bot_execution_selected_exchanges", true)}
                        >
                          Check all
                        </button>
                        <button
                          type="button"
                          style={{ padding: "4px 10px", minHeight: 28, fontSize: 12 }}
                          onClick={() => setExchangeListAll("bot_execution_selected_exchanges", false)}
                        >
                          Uncheck all
                        </button>
                      </div>
                      <div className="ml-exchange-grid">
                        {allExchangeOptions.map((option) => (
                          <label key={`botexec-${option.exchange_id}`} className="ml-exchange-item">
                            <input
                              type="checkbox"
                              checked={profileDraft.bot_execution_selected_exchanges.includes(option.exchange_id)}
                              onChange={(event) => toggleExchangeList("bot_execution_selected_exchanges", option.exchange_id, event.target.checked)}
                            />
                            <span>{option.name}</span>
                          </label>
                        ))}
                      </div>
                    </label>
                    <label>
                      Bot signal mode
                      <select
                        value={profileDraft.bot_signal_mode}
                        onChange={(event) =>
                          updateProfileField("bot_signal_mode", event.target.value === "majority" || event.target.value === "best" ? event.target.value : "combined")
                        }
                      >
                        <option value="combined">Combine all data</option>
                        <option value="majority">Majority decision</option>
                        <option value="best">Best exchange signal</option>
                      </select>
                    </label>
                    <label>
                      Execution position mode
                      <select
                        value={profileDraft.execution_position_mode}
                        onChange={(event) =>
                          updateProfileField(
                            "execution_position_mode",
                            event.target.value === "separate_positions" ? "separate_positions" : "combined_position"
                          )
                        }
                      >
                        <option value="combined_position">Combined position</option>
                        <option value="separate_positions">Separate positions</option>
                      </select>
                    </label>
                  </>
                ) : null}
                {profileModalSection === "pair" ? (
                  <>
                <div className="ml-note" style={{ gridColumn: "1 / -1", fontWeight: 700 }}>Data Sources</div>
                <label className="settings-checkbox-row">
                  <input
                    type="checkbox"
                    checked={profileDraft.use_local_data}
                    onChange={(event) => updateProfileField("use_local_data", event.target.checked)}
                  />
                  Use Local Data
                </label>
                <label className="settings-checkbox-row">
                  <input
                    type="checkbox"
                    checked={profileDraft.use_historic_data}
                    onChange={(event) => updateProfileField("use_historic_data", event.target.checked)}
                  />
                  Use Historic Data
                </label>
                <label>
                  Historic Data (Days)
                  <input
                    type="number"
                    step={1}
                    min={1}
                    value={profileDraft.historic_data_days}
                    disabled={!profileDraft.use_historic_data}
                    max={profileDraft.historic_data_days_max ?? 3650}
                    onChange={(event) => {
                      const maxDays = Math.max(1, Math.round(Number(profileDraft.historic_data_days_max ?? 3650)));
                      const next = Math.max(1, Math.min(maxDays, Math.round(Number(event.target.value || 90))));
                      updateProfileField("historic_data_days", next);
                    }}
                  />
                </label>
                {!profileDraft.use_local_data && !profileDraft.use_historic_data ? (
                  <div className="ml-error-text">At least one data source must be enabled for training.</div>
                ) : null}
                <div className="ml-note" style={{ gridColumn: "1 / -1", fontWeight: 700, marginTop: 6 }}>Labeling + Minimum Data</div>
                <label>
                  Target mode
                  <select
                    value={profileDraft.target_mode ?? "triple_barrier"}
                    onChange={(event) =>
                      updateProfileField(
                        "target_mode",
                        event.target.value === "fixed_return"
                          ? "fixed_return"
                          : event.target.value === "trade_outcome"
                            ? "trade_outcome"
                            : "triple_barrier",
                      )
                    }
                  >
                    <option value="triple_barrier">Triple barrier</option>
                    <option value="fixed_return">Fixed return (legacy)</option>
                    <option value="trade_outcome">Trade outcome (LONG_GOOD / SHORT_GOOD / NO_TRADE)</option>
                  </select>
                </label>
                <label>
                  Horizons (comma-separated seconds)
                  <input
                    value={profileDraft.horizons.join(",")}
                    onChange={(event) => {
                      const values = event.target.value
                        .split(",")
                        .map((item) => Number(item.trim()))
                        .filter((item) => Number.isFinite(item) && item > 0)
                        .map((item) => Math.round(item));
                      updateProfileField("horizons", values.length ? values : [5, 15, 30, 60]);
                    }}
                  />
                </label>
                <label>
                  Label threshold pct
                  <input
                    type="number"
                    step={0.001}
                    min={0.001}
                    value={profileDraft.label_threshold_pct}
                    onChange={(event) => updateProfileField("label_threshold_pct", Number(event.target.value || 0.05))}
                  />
                </label>
                <div className="ml-note" style={{ gridColumn: "1 / -1" }}>
                  Triple-barrier and advanced label guard controls are in <strong>Open Exp Settings</strong>.
                </div>
                <label>
                  Min data hours
                  <input
                    type="number"
                    step={0.1}
                    min={0.1}
                    value={profileDraft.min_data_hours}
                    onChange={(event) => updateProfileField("min_data_hours", Number(event.target.value || 48))}
                  />
                </label>
                <div className="ml-note" style={{ gridColumn: "1 / -1" }}>
                  Local-market-event full/strict/hour-window controls are in <strong>Open Exp Settings</strong>.
                </div>
                <div className="ml-note" style={{ gridColumn: "1 / -1", fontWeight: 700, marginTop: 6 }}>Training Core</div>
                <label>
                  Normalizer window
                  <input
                    type="number"
                    step={1}
                    min={30}
                    value={profileDraft.normalizer_window}
                    onChange={(event) => updateProfileField("normalizer_window", Number(event.target.value || 1000))}
                  />
                </label>
                <label>
                  Replay candle window
                  <input
                    type="number"
                    step={1}
                    min={30}
                    max={5000}
                    value={profileDraft.replay_candle_limit}
                    onChange={(event) => updateProfileField("replay_candle_limit", Number(event.target.value || 240))}
                  />
                </label>
                <label>
                  Epochs
                  <input
                    type="number"
                    step={1}
                    min={1}
                    value={profileDraft.training.epochs}
                    onChange={(event) => updateTrainingField("epochs", Number(event.target.value || 1))}
                  />
                </label>
                <label>
                  Batch size
                  <input
                    type="number"
                    step={1}
                    min={8}
                    value={profileDraft.training.batch_size}
                    onChange={(event) => updateTrainingField("batch_size", Number(event.target.value || 8))}
                  />
                </label>
                <label>
                  Learning rate
                  <input
                    type="number"
                    step={0.0001}
                    min={0.00001}
                    value={profileDraft.training.learning_rate}
                    onChange={(event) => updateTrainingField("learning_rate", Number(event.target.value || 0.001))}
                  />
                </label>
                <label>
                  Lookback steps
                  <input
                    type="number"
                    step={1}
                    min={10}
                    value={profileDraft.training.lookback_steps}
                    onChange={(event) => updateTrainingField("lookback_steps", Number(event.target.value || 60))}
                  />
                </label>
                <label>
                  Hidden size
                  <input
                    type="number"
                    step={1}
                    min={16}
                    value={profileDraft.training.hidden_size}
                    onChange={(event) => updateTrainingField("hidden_size", Number(event.target.value || 128))}
                  />
                </label>
                <label>
                  Num layers
                  <input
                    type="number"
                    step={1}
                    min={1}
                    value={profileDraft.training.num_layers}
                    onChange={(event) => updateTrainingField("num_layers", Number(event.target.value || 2))}
                  />
                </label>
                <label>
                  Dropout
                  <input
                    type="number"
                    step={0.01}
                    min={0}
                    max={0.9}
                    value={profileDraft.training.dropout}
                    onChange={(event) => updateTrainingField("dropout", Number(event.target.value || 0.2))}
                  />
                </label>
                <div className="ml-note" style={{ gridColumn: "1 / -1", fontWeight: 700, marginTop: 6 }}>Model Init + Class Imbalance</div>
                <label className="settings-checkbox-row">
                  <input
                    type="checkbox"
                    checked={Boolean(profileDraft.training.warm_start_from_current)}
                    onChange={(event) => updateTrainingField("warm_start_from_current", event.target.checked)}
                  />
                  Warm start from current approved model
                </label>
                <label className="settings-checkbox-row">
                  <input
                    type="checkbox"
                    checked={Boolean(profileDraft.training.use_class_weighted_loss)}
                    onChange={(event) => updateTrainingField("use_class_weighted_loss", event.target.checked)}
                  />
                  Use class-weighted direction loss
                </label>
                <label>
                  Class weight mode
                  <select
                    value={profileDraft.training.class_weight_mode}
                    disabled={!profileDraft.training.use_class_weighted_loss}
                    onChange={(event) =>
                      updateTrainingField(
                        "class_weight_mode",
                        event.target.value === "manual" ? "manual" : "auto"
                      )
                    }
                  >
                    <option value="auto">Auto (from class frequency)</option>
                    <option value="manual">Manual</option>
                  </select>
                </label>
                <label>
                  Weight DOWN
                  <input
                    type="number"
                    step={0.1}
                    min={0}
                    value={profileDraft.training.class_weight_down}
                    disabled={
                      !profileDraft.training.use_class_weighted_loss ||
                      profileDraft.training.class_weight_mode !== "manual"
                    }
                    onChange={(event) => updateTrainingField("class_weight_down", Number(event.target.value || 0))}
                  />
                </label>
                <label>
                  Weight FLAT
                  <input
                    type="number"
                    step={0.1}
                    min={0}
                    value={profileDraft.training.class_weight_flat}
                    disabled={
                      !profileDraft.training.use_class_weighted_loss ||
                      profileDraft.training.class_weight_mode !== "manual"
                    }
                    onChange={(event) => updateTrainingField("class_weight_flat", Number(event.target.value || 0))}
                  />
                </label>
                <label>
                  Weight UP
                  <input
                    type="number"
                    step={0.1}
                    min={0}
                    value={profileDraft.training.class_weight_up}
                    disabled={
                      !profileDraft.training.use_class_weighted_loss ||
                      profileDraft.training.class_weight_mode !== "manual"
                    }
                    onChange={(event) => updateTrainingField("class_weight_up", Number(event.target.value || 0))}
                  />
                </label>
                <label className="settings-checkbox-row">
                  <input
                    type="checkbox"
                    checked={Boolean(profileDraft.training.use_focal_loss)}
                    onChange={(event) => updateTrainingField("use_focal_loss", event.target.checked)}
                  />
                  Use focal loss (direction)
                </label>
                <label>
                  Focal gamma
                  <input
                    type="number"
                    step={0.1}
                    min={0}
                    value={profileDraft.training.focal_gamma}
                    disabled={!profileDraft.training.use_focal_loss}
                    onChange={(event) => updateTrainingField("focal_gamma", Number(event.target.value || 0))}
                  />
                </label>
                <label className="settings-checkbox-row">
                  <input
                    type="checkbox"
                    checked={Boolean(profileDraft.training.focal_use_alpha_class_weights)}
                    disabled={!profileDraft.training.use_focal_loss}
                    onChange={(event) =>
                      updateTrainingField("focal_use_alpha_class_weights", event.target.checked)
                    }
                  />
                  Focal alpha from class weights
                </label>
                <label className="settings-checkbox-row">
                  <input
                    type="checkbox"
                    checked={Boolean(profileDraft.training.use_balanced_sampler)}
                    onChange={(event) => updateTrainingField("use_balanced_sampler", event.target.checked)}
                  />
                  Use balanced sampler (direction classes)
                </label>
                <label className="settings-checkbox-row">
                  <input
                    type="checkbox"
                    checked={Boolean(profileDraft.training.use_class_quota_batches)}
                    onChange={(event) => updateTrainingField("use_class_quota_batches", event.target.checked)}
                  />
                  Use minimum class quota per batch
                </label>
                <label>
                  Class quota per batch (each class)
                  <input
                    type="number"
                    step={1}
                    min={0}
                    value={profileDraft.training.class_quota_per_batch}
                    disabled={!profileDraft.training.use_class_quota_batches}
                    onChange={(event) => updateTrainingField("class_quota_per_batch", Number(event.target.value || 0))}
                  />
                </label>
                <label>
                  Label smoothing
                  <input
                    type="number"
                    step={0.01}
                    min={0}
                    max={0.2}
                    value={profileDraft.training.label_smoothing}
                    onChange={(event) => updateTrainingField("label_smoothing", Number(event.target.value || 0))}
                  />
                </label>
                <div className="ml-note" style={{ gridColumn: "1 / -1", fontWeight: 700, marginTop: 6 }}>Early Stopping</div>
                <label className="settings-checkbox-row">
                  <input
                    type="checkbox"
                    checked={Boolean(profileDraft.training.early_stopping_enabled)}
                    onChange={(event) => updateTrainingField("early_stopping_enabled", event.target.checked)}
                  />
                  Enable early stopping
                </label>
                <label>
                  Early stopping patience
                  <input
                    type="number"
                    step={1}
                    min={1}
                    value={profileDraft.training.early_stopping_patience}
                    onChange={(event) => updateTrainingField("early_stopping_patience", Number(event.target.value || 1))}
                    disabled={!profileDraft.training.early_stopping_enabled}
                  />
                </label>
                <label>
                  Early stopping monitor
                  <select
                    value={profileDraft.training.early_stopping_monitor}
                    disabled={!profileDraft.training.early_stopping_enabled}
                    onChange={(event) =>
                      updateTrainingField(
                        "early_stopping_monitor",
                        event.target.value === "macro_f1" ? "macro_f1" : "val_loss"
                      )
                    }
                  >
                    <option value="val_loss">Validation loss</option>
                    <option value="macro_f1">Validation macro F1</option>
                  </select>
                </label>
                <label>
                  Early stopping min delta
                  <input
                    type="number"
                    step={0.0001}
                    min={0}
                    value={profileDraft.training.early_stopping_min_delta}
                    onChange={(event) => updateTrainingField("early_stopping_min_delta", Number(event.target.value || 0))}
                    disabled={!profileDraft.training.early_stopping_enabled}
                  />
                </label>
                  </>
                ) : null}
                {profileModalSection === "trace" ? (
                  <>
                <div className="ml-note" style={{ gridColumn: "1 / -1", fontWeight: 700, marginTop: 6 }}>Replay Trace Capture</div>
                <label>
                  Epoch trace capture list (e.g. 1,4,6)
                  <input
                    value={String(profileDraft.training.epoch_trace_capture_list ?? "")}
                    onChange={(event) => updateTrainingField("epoch_trace_capture_list", event.target.value)}
                  />
                </label>
                <label className="settings-checkbox-row">
                  <input
                    type="checkbox"
                    checked={Boolean(profileDraft.training.capture_trace_train)}
                    onChange={(event) => updateTrainingField("capture_trace_train", event.target.checked)}
                  />
                  Capture replay trace: Train split
                </label>
                <label className="settings-checkbox-row">
                  <input
                    type="checkbox"
                    checked={Boolean(profileDraft.training.capture_trace_val)}
                    onChange={(event) => updateTrainingField("capture_trace_val", event.target.checked)}
                  />
                  Capture replay trace: Val split
                </label>
                <label className="settings-checkbox-row">
                  <input
                    type="checkbox"
                    checked={Boolean(profileDraft.training.capture_trace_test)}
                    onChange={(event) => updateTrainingField("capture_trace_test", event.target.checked)}
                  />
                  Capture replay trace: Test split
                </label>
                {!profileDraft.training.capture_trace_train &&
                !profileDraft.training.capture_trace_val &&
                !profileDraft.training.capture_trace_test ? (
                  <div className="warn-text">Replay disabled - no split selected.</div>
                ) : null}
                <label>
                  Replay candle limit
                  <input
                    type="number"
                    step={1}
                    min={30}
                    max={5000}
                    value={profileDraft.replay_candle_limit}
                    onChange={(event) => updateProfileField("replay_candle_limit", Number(event.target.value || 240))}
                  />
                </label>
                  </>
                ) : null}
                {profileModalSection === "exp" ? (
                  <>
                    <div className="ml-note" style={{ gridColumn: "1 / -1", fontWeight: 700, marginTop: 6 }}>Labeling Experiments</div>
                    <label>
                      Triple barrier TP %
                      <input
                        type="number"
                        step={0.01}
                        min={0.01}
                        value={profileDraft.triple_barrier?.tp_pct ?? 0.08}
                        onChange={(event) =>
                          setProfileDraft((prev) => {
                            if (!prev) return prev;
                            return {
                              ...prev,
                              triple_barrier: {
                                ...(prev.triple_barrier ?? { tp_pct: 0.08, sl_pct: 0.05, timeout_steps: 180 }),
                                tp_pct: Number(event.target.value || 0.08),
                              },
                            };
                          })
                        }
                      />
                    </label>
                    <label>
                      Triple barrier SL %
                      <input
                        type="number"
                        step={0.01}
                        min={0.01}
                        value={profileDraft.triple_barrier?.sl_pct ?? 0.05}
                        onChange={(event) =>
                          setProfileDraft((prev) => {
                            if (!prev) return prev;
                            return {
                              ...prev,
                              triple_barrier: {
                                ...(prev.triple_barrier ?? { tp_pct: 0.08, sl_pct: 0.05, timeout_steps: 180 }),
                                sl_pct: Number(event.target.value || 0.05),
                              },
                            };
                          })
                        }
                      />
                    </label>
                    <label>
                      Triple barrier timeout (steps)
                      <input
                        type="number"
                        step={1}
                        min={1}
                        value={profileDraft.triple_barrier?.timeout_steps ?? 180}
                        onChange={(event) =>
                          setProfileDraft((prev) => {
                            if (!prev) return prev;
                            return {
                              ...prev,
                              triple_barrier: {
                                ...(prev.triple_barrier ?? { tp_pct: 0.08, sl_pct: 0.05, timeout_steps: 180 }),
                                timeout_steps: Math.max(1, Math.round(Number(event.target.value || 180))),
                              },
                            };
                          })
                        }
                      />
                    </label>
                    <label>
                      Min FLAT rate required %
                      <input
                        type="number"
                        step={1}
                        min={0}
                        max={100}
                        value={Math.round((profileDraft.paper_bot?.min_flat_rate_required ?? 0.12) * 100)}
                        onChange={(event) =>
                          updatePaperBotField("min_flat_rate_required", Math.max(0, Math.min(1, Number(event.target.value || 12) / 100)))
                        }
                      />
                    </label>
                    <label>
                      Max label-price rejected rate %
                      <input
                        type="number"
                        step={0.01}
                        min={0}
                        max={100}
                        value={Number((profileDraft.paper_bot?.max_label_price_rejected_rate ?? 0.005) * 100).toFixed(2)}
                        onChange={(event) =>
                          updatePaperBotField(
                            "max_label_price_rejected_rate",
                            Math.max(0, Math.min(1, Number(event.target.value || 0.5) / 100)),
                          )
                        }
                      />
                    </label>
                    <label className="settings-checkbox-row" style={{ gridColumn: "1 / -1" }}>
                      <input
                        type="checkbox"
                        checked={Boolean(profileDraft.label_guard_enabled ?? true)}
                        onChange={(event) => updateProfileField("label_guard_enabled", event.target.checked)}
                      />
                      Fail run if FLAT safety check fails (Debug only: disabling may train on invalid labels)
                    </label>
                    <div className="ml-note" style={{ gridColumn: "1 / -1", fontWeight: 700, marginTop: 6 }}>Data Window Experiments</div>
                    <label className="settings-checkbox-row">
                      <input
                        type="checkbox"
                        checked={Boolean(profileDraft.full_data_mode)}
                        onChange={(event) => updateProfileField("full_data_mode", event.target.checked)}
                      />
                      Full Data Mode (use full event range + dense sampling)
                    </label>
                    <label className="settings-checkbox-row">
                      <input
                        type="checkbox"
                        checked={Boolean(profileDraft.strict_full_windows_mode)}
                        onChange={(event) => updateProfileField("strict_full_windows_mode", event.target.checked)}
                      />
                      Strict Full Windows Mode (use all windows, no memory downsampling)
                    </label>
                    <label className="settings-checkbox-row">
                      <input
                        type="checkbox"
                        checked={Boolean(profileDraft.training_hour_window_enabled)}
                        onChange={(event) => updateProfileField("training_hour_window_enabled", event.target.checked)}
                      />
                      Use training hour window (local market events only)
                    </label>
                    <label>
                      From hour (0 = from data start)
                      <input
                        type="number"
                        step={1}
                        min={0}
                        disabled={!profileDraft.training_hour_window_enabled}
                        value={profileDraft.training_hour_start}
                        onChange={(event) => {
                          const next = Math.max(0, Math.round(Number(event.target.value || 0)));
                          updateProfileField("training_hour_start", next);
                        }}
                      />
                    </label>
                    <label>
                      To hour (0 = until data end)
                      <input
                        type="number"
                        step={1}
                        min={0}
                        disabled={!profileDraft.training_hour_window_enabled}
                        value={profileDraft.training_hour_end}
                        onChange={(event) => {
                          const next = Math.max(0, Math.round(Number(event.target.value || 0)));
                          updateProfileField("training_hour_end", next);
                        }}
                      />
                    </label>
                    <div className="ml-note" style={{ gridColumn: "1 / -1" }}>
                      Available local data hours: {Number.isFinite(totalLocalHoursAvailable) ? totalLocalHoursAvailable.toFixed(2) : "0.00"}.
                    </div>
                    <div className="ml-note" style={{ gridColumn: "1 / -1", fontWeight: 700, marginTop: 6 }}>Barrier Debug Controls</div>
                    <label className="settings-checkbox-row">
                      <input
                        type="checkbox"
                        checked={Boolean(profileDraft.barrier_debug?.enable_future_path_probe ?? true)}
                        onChange={(event) =>
                          setProfileDraft((prev) => {
                            if (!prev) return prev;
                            return {
                              ...prev,
                              barrier_debug: {
                                ...(prev.barrier_debug ?? {
                                  enable_future_path_probe: true,
                                  future_path_probe_samples: 5,
                                  future_path_probe_depth: 20,
                                  step_contiguity_tolerance_ms: 500,
                                  timeout_unit_hint: "steps",
                                }),
                                enable_future_path_probe: event.target.checked,
                              },
                            };
                          })
                        }
                      />
                      Enable future-path probe
                    </label>
                    <label>
                      Future-path probe samples
                      <input
                        type="number"
                        step={1}
                        min={1}
                        max={20}
                        value={profileDraft.barrier_debug?.future_path_probe_samples ?? 5}
                        onChange={(event) =>
                          setProfileDraft((prev) => {
                            if (!prev) return prev;
                            return {
                              ...prev,
                              barrier_debug: {
                                ...(prev.barrier_debug ?? {
                                  enable_future_path_probe: true,
                                  future_path_probe_samples: 5,
                                  future_path_probe_depth: 20,
                                  step_contiguity_tolerance_ms: 500,
                                  timeout_unit_hint: "steps",
                                }),
                                future_path_probe_samples: Math.max(1, Math.min(20, Math.round(Number(event.target.value || 5)))),
                              },
                            };
                          })
                        }
                      />
                    </label>
                    <label>
                      Future-path probe depth
                      <input
                        type="number"
                        step={1}
                        min={5}
                        max={200}
                        value={profileDraft.barrier_debug?.future_path_probe_depth ?? 20}
                        onChange={(event) =>
                          setProfileDraft((prev) => {
                            if (!prev) return prev;
                            return {
                              ...prev,
                              barrier_debug: {
                                ...(prev.barrier_debug ?? {
                                  enable_future_path_probe: true,
                                  future_path_probe_samples: 5,
                                  future_path_probe_depth: 20,
                                  step_contiguity_tolerance_ms: 500,
                                  timeout_unit_hint: "steps",
                                }),
                                future_path_probe_depth: Math.max(5, Math.min(200, Math.round(Number(event.target.value || 20)))),
                              },
                            };
                          })
                        }
                      />
                    </label>
                    <label>
                      Step contiguity tolerance (ms)
                      <input
                        type="number"
                        step={1}
                        min={1}
                        max={60000}
                        value={profileDraft.barrier_debug?.step_contiguity_tolerance_ms ?? 500}
                        onChange={(event) =>
                          setProfileDraft((prev) => {
                            if (!prev) return prev;
                            return {
                              ...prev,
                              barrier_debug: {
                                ...(prev.barrier_debug ?? {
                                  enable_future_path_probe: true,
                                  future_path_probe_samples: 5,
                                  future_path_probe_depth: 20,
                                  step_contiguity_tolerance_ms: 500,
                                  timeout_unit_hint: "steps",
                                }),
                                step_contiguity_tolerance_ms: Math.max(1, Math.min(60000, Math.round(Number(event.target.value || 500)))),
                              },
                            };
                          })
                        }
                      />
                    </label>
                    <label>
                      Timeout unit hint (UI only)
                      <select
                        value={profileDraft.barrier_debug?.timeout_unit_hint ?? "steps"}
                        onChange={(event) =>
                          setProfileDraft((prev) => {
                            if (!prev) return prev;
                            return {
                              ...prev,
                              barrier_debug: {
                                ...(prev.barrier_debug ?? {
                                  enable_future_path_probe: true,
                                  future_path_probe_samples: 5,
                                  future_path_probe_depth: 20,
                                  step_contiguity_tolerance_ms: 500,
                                  timeout_unit_hint: "steps",
                                }),
                                timeout_unit_hint: event.target.value === "seconds_hint" ? "seconds_hint" : "steps",
                              },
                            };
                          })
                        }
                      >
                        <option value="steps">Steps</option>
                        <option value="seconds_hint">Seconds hint</option>
                      </select>
                    </label>
                    <div className="ml-note" style={{ gridColumn: "1 / -1", fontWeight: 700, marginTop: 6 }}>Confidence Gate Controls</div>
                    <label>
                      Gate mode
                      <select
                        value={profileDraft.paper_bot.gate_mode ?? "confidence_only"}
                        onChange={(event) => updatePaperBotField("gate_mode", event.target.value === "combined" ? "combined" : "confidence_only")}
                      >
                        <option value="confidence_only">confidence_only</option>
                        <option value="combined">combined</option>
                      </select>
                    </label>
                    <label>
                      Entry confidence threshold
                      <input type="number" step={0.01} min={0} max={1} value={profileDraft.paper_bot.entry_confidence_threshold ?? 0.72} onChange={(event) => updatePaperBotField("entry_confidence_threshold", Number(event.target.value || 0.72))} />
                    </label>
                    <label>
                      Exit confidence threshold
                      <input type="number" step={0.01} min={0} max={1} value={profileDraft.paper_bot.exit_confidence_threshold ?? 0.55} onChange={(event) => updatePaperBotField("exit_confidence_threshold", Number(event.target.value || 0.55))} />
                    </label>
                    <label>
                      Minimum directional edge
                      <input type="number" step={0.01} min={0} max={1} value={profileDraft.paper_bot.minimum_directional_edge ?? 0.03} onChange={(event) => updatePaperBotField("minimum_directional_edge", Number(event.target.value || 0.03))} />
                    </label>
                    <label className="settings-checkbox-row">
                      <input type="checkbox" checked={Boolean(profileDraft.paper_bot.temperature_scaling_enabled ?? true)} onChange={(event) => updatePaperBotField("temperature_scaling_enabled", event.target.checked)} />
                      Temperature scaling enabled
                    </label>
                    <label>
                      Minimum action rate
                      <input type="number" step={0.001} min={0} max={1} value={profileDraft.paper_bot.minimum_action_rate ?? 0.03} onChange={(event) => updatePaperBotField("minimum_action_rate", Number(event.target.value || 0.03))} />
                    </label>
                    <label>
                      Minimum directional samples
                      <input type="number" step={1} min={1} value={profileDraft.paper_bot.minimum_directional_samples ?? 30} onChange={(event) => updatePaperBotField("minimum_directional_samples", Number(event.target.value || 30))} />
                    </label>
                    <label>
                      Estimated roundtrip cost (bps)
                      <input type="number" step={0.1} min={0} value={profileDraft.paper_bot.estimated_roundtrip_cost_bps ?? 6} onChange={(event) => updatePaperBotField("estimated_roundtrip_cost_bps", Number(event.target.value || 6))} />
                    </label>
                    <label>
                      Max signal age (ms)
                      <input type="number" step={100} min={0} value={profileDraft.paper_bot.max_signal_age_ms ?? 2000} onChange={(event) => updatePaperBotField("max_signal_age_ms", Number(event.target.value || 2000))} />
                    </label>
                    <label className="settings-checkbox-row">
                      <input type="checkbox" checked={Boolean(profileDraft.paper_bot.volatility_guard_enabled ?? false)} onChange={(event) => updatePaperBotField("volatility_guard_enabled", event.target.checked)} />
                      Volatility guard enabled
                    </label>
                    <label>
                      Max realized volatility
                      <input type="number" step={0.001} min={0} value={profileDraft.paper_bot.max_realized_volatility ?? 0.02} onChange={(event) => updatePaperBotField("max_realized_volatility", Number(event.target.value || 0.02))} />
                    </label>
                    <label>
                      Volatility guard action
                      <select value={profileDraft.paper_bot.volatility_guard_action ?? "hold_only"} onChange={(event) => updatePaperBotField("volatility_guard_action", event.target.value === "raise_threshold_multiplier" ? "raise_threshold_multiplier" : "hold_only")}>
                        <option value="hold_only">hold_only</option>
                        <option value="raise_threshold_multiplier">raise_threshold_multiplier</option>
                      </select>
                    </label>
                    <label>
                      Max spread (bps)
                      <input type="number" step={0.1} min={0} value={profileDraft.paper_bot.max_spread_bps ?? 15} onChange={(event) => updatePaperBotField("max_spread_bps", Number(event.target.value || 15))} />
                    </label>
                    <label>
                      Min orderbook depth
                      <input type="number" step={1} min={0} value={profileDraft.paper_bot.min_orderbook_depth ?? 0} onChange={(event) => updatePaperBotField("min_orderbook_depth", Number(event.target.value || 0))} />
                    </label>
                    <label>
                      Live calibration guard action
                      <select value={profileDraft.paper_bot.live_calibration_guard_action ?? "monitor_only"} onChange={(event) => updatePaperBotField("live_calibration_guard_action", event.target.value === "block_entries" ? "block_entries" : "monitor_only")}>
                        <option value="monitor_only">monitor_only</option>
                        <option value="block_entries">block_entries</option>
                      </select>
                    </label>
                    <label>
                      Max trades per hour
                      <input type="number" step={1} min={0} value={profileDraft.paper_bot.max_trades_per_hour ?? 30} onChange={(event) => updatePaperBotField("max_trades_per_hour", Number(event.target.value || 30))} />
                    </label>
                    <label>
                      Max consecutive entries
                      <input type="number" step={1} min={0} value={profileDraft.paper_bot.max_consecutive_entries ?? 5} onChange={(event) => updatePaperBotField("max_consecutive_entries", Number(event.target.value || 5))} />
                    </label>
                    <label>
                      Min seconds between entries
                      <input type="number" step={1} min={0} value={profileDraft.paper_bot.min_seconds_between_entries ?? 5} onChange={(event) => updatePaperBotField("min_seconds_between_entries", Number(event.target.value || 5))} />
                    </label>
                  </>
                ) : null}
                {profileModalSection === "bot" ? (
                  <>
                <label className="settings-checkbox-row">
                  <input
                    type="checkbox"
                    checked={profileDraft.paper_bot.enabled}
                    onChange={(event) => updatePaperBotField("enabled", event.target.checked)}
                  />
                  Enable paper bot
                </label>
                <label>
                  Confidence threshold
                  <input
                    type="number"
                    step={0.01}
                    min={0}
                    max={1}
                    value={profileDraft.paper_bot.confidence_threshold}
                    onChange={(event) => updatePaperBotField("confidence_threshold", Number(event.target.value || 0.72))}
                  />
                </label>
                <label>
                  Quality threshold
                  <input
                    type="number"
                    step={0.01}
                    min={0}
                    max={1}
                    value={profileDraft.paper_bot.quality_threshold}
                    onChange={(event) => updatePaperBotField("quality_threshold", Number(event.target.value || 0.72))}
                  />
                </label>
                <label>
                  Bot feed delay (ms)
                  <input
                    type="range"
                    min={100}
                    max={20000}
                    step={100}
                    value={Number(profileDraft.paper_bot.poll_delay_ms ?? 2000)}
                    onChange={(event) =>
                      updatePaperBotField(
                        "poll_delay_ms",
                        Math.max(100, Math.min(20000, Number(event.target.value || 2000))),
                      )
                    }
                  />
                  <div className="settings-note">
                    {Math.max(100, Math.min(20000, Number(profileDraft.paper_bot.poll_delay_ms ?? 2000)))} ms
                  </div>
                </label>
                <label>
                  Max position qty
                  <input
                    type="number"
                    step={0.1}
                    min={0.1}
                    value={profileDraft.paper_bot.max_position_qty}
                    onChange={(event) => updatePaperBotField("max_position_qty", Number(event.target.value || 1))}
                  />
                </label>
                <label>
                  Initial balance
                  <input
                    type="number"
                    step={1}
                    min={1}
                    value={profileDraft.paper_bot.initial_balance}
                    onChange={(event) => updatePaperBotField("initial_balance", Number(event.target.value || 100))}
                  />
                </label>
                <label>
                  Max hold seconds
                  <input
                    type="number"
                    step={1}
                    min={10}
                    value={profileDraft.paper_bot.max_hold_seconds}
                    onChange={(event) => updatePaperBotField("max_hold_seconds", Number(event.target.value || 120))}
                  />
                </label>
                <label>
                  Leverage
                  <input
                    type="number"
                    step={0.1}
                    min={1}
                    value={profileDraft.paper_bot.leverage}
                    onChange={(event) => updatePaperBotField("leverage", Number(event.target.value || 5))}
                  />
                </label>
                <label>
                  Take profit (%)
                  <input
                    type="number"
                    step={0.01}
                    min={0}
                    value={profileDraft.paper_bot.take_profit_pct}
                    onChange={(event) => updatePaperBotField("take_profit_pct", Number(event.target.value || 0))}
                  />
                </label>
                <label>
                  Stop loss (%)
                  <input
                    type="number"
                    step={0.01}
                    min={0}
                    value={profileDraft.paper_bot.stop_loss_pct}
                    onChange={(event) => updatePaperBotField("stop_loss_pct", Number(event.target.value || 0))}
                  />
                </label>
                <label className="settings-checkbox-row">
                  <input
                    type="checkbox"
                    checked={Boolean(profileDraft.paper_bot.use_trailing_stop)}
                    onChange={(event) => updatePaperBotField("use_trailing_stop", event.target.checked)}
                  />
                  Use trailing stop
                </label>
                <label>
                  Trailing stop (%)
                  <input
                    type="number"
                    step={0.01}
                    min={0}
                    value={profileDraft.paper_bot.trailing_stop_pct}
                    onChange={(event) => updatePaperBotField("trailing_stop_pct", Number(event.target.value || 0))}
                  />
                </label>
                <label>
                  Commission fee per trade (%)
                  <input
                    type="number"
                    step={0.001}
                    min={0}
                    value={profileDraft.paper_bot.commission_fee_pct}
                    onChange={(event) => updatePaperBotField("commission_fee_pct", Number(event.target.value || 0))}
                  />
                </label>
                <label className="settings-checkbox-row">
                  <input
                    type="checkbox"
                    checked={Boolean(profileDraft.paper_bot.one_trade_at_time)}
                    onChange={(event) => updatePaperBotField("one_trade_at_time", event.target.checked)}
                  />
                  One trade at a time
                </label>
                <label className="settings-checkbox-row">
                  <input
                    type="checkbox"
                    checked={Boolean(profileDraft.paper_bot.regime_filter_enabled)}
                    onChange={(event) => updatePaperBotField("regime_filter_enabled", event.target.checked)}
                  />
                  Enable no-trade regime filter
                </label>
                <label>
                  Max spread (%)
                  <input
                    type="number"
                    step={0.01}
                    min={0}
                    value={profileDraft.paper_bot.max_spread_pct}
                    onChange={(event) => updatePaperBotField("max_spread_pct", Number(event.target.value || 0))}
                  />
                </label>
                <label>
                  Min trade speed (10s)
                  <input
                    type="number"
                    step={0.1}
                    min={0}
                    value={profileDraft.paper_bot.min_trade_rate_10s}
                    onChange={(event) => updatePaperBotField("min_trade_rate_10s", Number(event.target.value || 0))}
                  />
                </label>
                <label>
                  Min confidence gap
                  <input
                    type="number"
                    step={0.01}
                    min={0}
                    max={1}
                    value={profileDraft.paper_bot.min_confidence_gap}
                    onChange={(event) => updatePaperBotField("min_confidence_gap", Number(event.target.value || 0))}
                  />
                </label>
                  </>
                ) : null}
              </div>
            ) : (
              <div className="ml-note">Select a pair to edit profile.</div>
            )}
            {renderProfileModalActions()}
          </section>
        </div>
      ) : null}

      {settingsView === "workspace_presets" ? (
      <section className="panel settings-preset-panel">
        <div className="settings-section-topbar">
          <button type="button" onClick={() => onSettingsViewChange("hub")}>Back</button>
        </div>
        <div className="small-title">Workspace Presets</div>
        <div className="kv"><span>Active preset</span><span>{presetStore.activePreset}</span></div>
        <div className="settings-preset-grid">
          <label>
            Timeframe
            <select
              value={draft.timeframe}
              onChange={(event) => setDraft((prev) => ({ ...prev, timeframe: event.target.value }))}
            >
              {TIMEFRAME_OPTIONS.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </label>
          <label>
            Heatmap Low
            <input
              type="number"
              min={0}
              max={1}
              step={0.01}
              value={draft.thresholdLow}
              onChange={(event) =>
                setDraft((prev) => ({ ...prev, thresholdLow: Number(event.target.value || 0) }))
              }
            />
          </label>
          <label>
            Heatmap High
            <input
              type="number"
              min={0}
              max={3}
              step={0.01}
              value={draft.thresholdHigh}
              onChange={(event) =>
                setDraft((prev) => ({ ...prev, thresholdHigh: Number(event.target.value || 0) }))
              }
            />
          </label>
          <label>
            Row Size
            <input
              type="number"
              min={1}
              max={50}
              step={1}
              value={draft.rowSizeMultiplier}
              onChange={(event) =>
                setDraft((prev) => ({ ...prev, rowSizeMultiplier: Number(event.target.value || 1) }))
              }
            />
          </label>
          <label>
            Heatmap Range
            <input
              type="number"
              min={1}
              max={500}
              step={1}
              value={draft.heatmapCoverageScale}
              onChange={(event) =>
                setDraft((prev) => ({ ...prev, heatmapCoverageScale: Number(event.target.value || 1) }))
              }
            />
          </label>
          <label className="settings-checkbox-row">
            <input
              type="checkbox"
              checked={draft.showHeatmap}
              onChange={(event) => setDraft((prev) => ({ ...prev, showHeatmap: event.target.checked }))}
            />
            Show Heatmap
          </label>
        </div>

        <div className="settings-preset-actions">
          <button onClick={handleSaveActive}>Save Active</button>
          <input
            value={newPresetName}
            onChange={(event) => setNewPresetName(event.target.value)}
            placeholder="New preset name"
          />
          <button onClick={handleCreatePreset}>Create From Draft</button>
        </div>

        <div className="settings-preset-list">
          {Object.keys(presetStore.presets).map((name) => (
            <div key={name} className="settings-preset-row">
              <span>{name}</span>
              <div>
                <button onClick={() => handleActivate(name)}>Activate</button>
                <button onClick={() => handleDeletePreset(name)}>Delete</button>
              </div>
            </div>
          ))}
        </div>
        {status ? <div className="settings-status">{status}</div> : null}
      </section>
      ) : null}

      {storageDeleteOpen ? (
        <div className="coverage-modal-backdrop" onClick={closeDeleteModal}>
          <section className="panel coverage-modal storage-delete-modal" onClick={(event) => event.stopPropagation()}>
            <div className="small-title">Delete Warning</div>
            <p className="coverage-error" style={{ marginTop: 0 }}>
              This permanently deletes data from disk/database and cannot be undone.
            </p>
            <div className="ml-note">Targets: {storageDeleteTargets.length}</div>
            <div className="storage-delete-list">
              {storageDeleteTargets.map((entry) => (
                <div key={`del-${entry.entry_id}`} className="storage-delete-row">
                  <strong>{entry.name}</strong>
                  <span>{entry.category_id}</span>
                  <code>{entry.path}</code>
                </div>
              ))}
            </div>
            {storageDeleteProgress ? (
              <div className="ml-note">
                {storageDeleteProgress}
                {storageDeleteJobId ? ` (job: ${storageDeleteJobId})` : ""}
              </div>
            ) : null}
            <div className="ml-subpanel-actions">
              <button type="button" className="danger" disabled={storageBusy} onClick={() => void confirmStorageDelete()}>
                {storageBusy ? "Deleting..." : "Confirm Permanent Delete"}
              </button>
              <button type="button" disabled={storageBusy} onClick={closeDeleteModal}>
                Cancel
              </button>
            </div>
          </section>
        </div>
      ) : null}
    </>
  );
}



