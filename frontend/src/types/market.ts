export type MarketEvent = {
  type: "market_event";
  symbol?: string;
  stream: string;
  event_type: string;
  received_at: string;
  payload: Record<string, unknown>;
};

export type KlinePoint = {
  open_ts_ms: number;
  close_ts_ms: number;
  open: number;
  high: number;
  low: number;
  close: number;
  interval: string;
  is_closed: boolean;
};

export type TerminalSnapshot = {
  type: "terminal_snapshot";
  symbol: string;
  received_at: string;
  event_count: number;
  book_synced: boolean;
  last_update_id: number;
  best_bid: number;
  best_ask: number;
  spread: number;
  mark_price: number;
  funding_rate: number;
  last_trade_price: number;
  last_trade_qty: number;
  last_trade_side: string;
  last_kline_close: number;
  last_kline_interval: string;
  last_liquidation_side: string;
  last_liquidation_qty: number;
  trade_rate_10s: number;
  liq_events_60s: number;
  sync_failures: number;
  return_5s?: number;
  return_15s?: number;
  return_30s?: number;
  return_60s?: number;
  rolling_volatility_30s?: number;
  rolling_volatility_60s?: number;
  candle_range_pct?: number;
  atr_short?: number;
  ema_fast_distance_pct?: number;
  ema_slow_distance_pct?: number;
  trend_slope_short?: number;
  vwap_distance_pct?: number;
  market_buy_volume_10s?: number;
  market_sell_volume_10s?: number;
  aggressive_buy_sell_delta?: number;
  cancel_rate_orderbook?: number;
  wall_strength_bid?: number;
  wall_strength_ask?: number;
  wall_persistence_seconds?: number;
  spread_change_rate?: number;
  volume_10s?: number;
  volume_30s?: number;
  relative_volume_ratio?: number;
  hour_of_day_sin?: number;
  hour_of_day_cos?: number;
  session_asia_eu_us?: number;
  open_interest_change_pct?: number;
  oi_velocity?: number;
  funding_rate_change?: number;
  stream_counts: Record<string, number>;
  depth_levels: {
    bids: number;
    asks: number;
  };
  decision_layer?: Record<string, unknown>;
};

export type SymbolChangedMessage = {
  type: "symbol_changed";
  symbol: string;
  received_at: string;
};

export type HeatmapCell = {
  row: number;
  value: number;
};

export type HeatmapColumn = {
  ts_ms: number;
  center_row: number;
  row_min: number;
  row_max: number;
  rows: HeatmapCell[];
};

export type HeatmapFrame = {
  type: "heatmap_frame";
  symbol: string;
  ts_ms: number;
  bucket_size: number;
  ticks_per_bucket: number;
  time_bucket_ms: number;
  max_columns: number;
  column: HeatmapColumn;
};

export type LadderRow = {
  row: number;
  price: number;
  liquidity: number;
  side: "above" | "below" | "at";
};

export type LadderSnapshot = {
  type: "ladder_snapshot";
  symbol: string;
  ts_ms: number;
  current_price: number;
  bucket_size: number;
  rows: LadderRow[];
};

export type HeatmapBootstrap = {
  type: "heatmap_bootstrap";
  symbol: string;
  bucket_size: number;
  ticks_per_bucket: number;
  time_bucket_ms: number;
  max_columns: number;
  columns: HeatmapColumn[];
  ladder: LadderSnapshot | null;
  current_price: number;
};

export type HeatmapViewportState = {
  follow_live: boolean;
  column_width: number;
  offset_columns: number;
};

export type HeatmapThresholdState = {
  low: number;
  high: number;
};

export type HeatmapInspectorState = {
  locked: boolean;
  column_index: number | null;
  row_index: number | null;
};

export type ChartHorizontalLine = {
  price: number;
  color: string;
};

export type ChartViewportState = {
  x_offset_bars: number;
  x_pixels_per_bar: number;
  right_padding_bars: number;
  follow_live: boolean;
  y_min: number;
  y_max: number;
  y_mode: "auto" | "manual";
};

export type ChartInteractionState = {
  crosshair_x: number | null;
  crosshair_y: number | null;
  active_bar_index: number | null;
  active_price: number | null;
  is_dragging: boolean;
  drag_target: "plot" | "price_axis" | null;
};

export type ExchangeCoverageItem = {
  exchange_id: string;
  name: string;
  implemented: boolean;
  enabled_by_user: boolean;
  supports_pair: boolean;
  connected: boolean;
  reason: string;
  last_update_age_sec: number | null;
  features: {
    book: boolean;
    trades: boolean;
    candles: boolean;
    mark_funding: boolean;
    liquidations: boolean;
  };
};

export type SourceCoverageUpdate = {
  type: "source_coverage_update";
  symbol: string;
  scope: "USDT_PERPETUAL";
  total_catalog_exchanges: number;
  supported_exchanges: number;
  enabled_exchanges: number;
  enabled_exchange_ids: string[];
  connected_exchanges: number;
  feature_contributors: {
    book: number;
    trades: number;
    candles: number;
    mark_funding: number;
    liquidations: number;
  };
  exchanges: ExchangeCoverageItem[];
  updated_at: string;
};

export type SymbolResolutionMessage = {
  type: "symbol_resolution";
  requested_symbol: string;
  normalized_symbol: string;
  scope: "USDT_PERPETUAL";
  found_exchanges: string[];
  rejected_exchanges: Array<{ exchange_id: string; reason: string }>;
  resolved_at: string;
};

export type SourceCoverageSelectionResponse = {
  ok: boolean;
  enabled_exchange_ids: string[];
  source_coverage: SourceCoverageUpdate;
};

export type MlRunUpdateMessage = {
  type: "ml_run_update";
  run: Record<string, unknown>;
};

export type MlRunLogMessage = {
  type: "ml_run_log";
  run_id: string;
  ts: string;
  level: string;
  message: string;
};

export type MlBotUpdateMessage = {
  type: "ml_bot_update";
  payload: Record<string, unknown>;
};

export type MlTrainingEpochMessage = {
  type: "ml_training_epoch";
  run_id: string;
  pair_symbol: string;
  instance_id: string;
  payload: Record<string, unknown>;
};

export type MlTrainingBatchMessage = {
  type: "ml_training_batch";
  run_id: string;
  pair_symbol: string;
  instance_id: string;
  payload: Record<string, unknown>;
};

export type RunEpochMetricsSeries = {
  run_id: string;
  pair_symbol: string;
  instance_id: string;
  status: string;
  epochs: Array<Record<string, unknown>>;
  captured_epochs?: number[];
  captured_epochs_by_split?: Record<string, number[]>;
  epoch_summaries?: Array<Record<string, unknown>>;
  prediction_trace: ReplayDecisionFrame[];
  final_metrics: Record<string, unknown>;
};

export type ReplayDecisionFrame = {
  step_index?: number;
  sample_index?: number;
  ts_ms?: number;
  price_at_prediction?: number;
  action?: string;
  predicted_label?: string;
  actual_label?: string;
  prob_down?: number;
  prob_flat?: number;
  prob_up?: number;
  confidence?: number;
  quality?: number;
  confidence_gap?: number;
  predicted_magnitude_pct?: number;
  predicted_abs_move_pct?: number;
  actual_future_return_pct?: number;
  magnitude_pct?: number;
  entry_allowed?: boolean;
  gate_result?: string;
  blocked_reason?: string;
  correct?: boolean;
  epoch?: number;
  ts_key?: number;
};

export type ReplayStepFrame = {
  replay_id: string;
  status: string;
  cursor: number;
  frames_total: number;
  source_mode?: string;
  epoch_index?: number;
  split?: string;
  frame: ReplayDecisionFrame | null;
  terminal_snapshot?: Record<string, unknown>;
  action_totals?: Record<string, number>;
  action_seen?: Record<string, number>;
  unavailable_fields?: string[];
};

export type TerminalWsMessage =
  | MarketEvent
  | TerminalSnapshot
  | SymbolChangedMessage
  | HeatmapFrame
  | LadderSnapshot
  | SourceCoverageUpdate
  | SymbolResolutionMessage
  | MlRunUpdateMessage
  | MlRunLogMessage
  | MlBotUpdateMessage
  | MlTrainingEpochMessage
  | MlTrainingBatchMessage;
