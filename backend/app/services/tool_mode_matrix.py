from __future__ import annotations

from typing import Any

TOOL_IDS = (
    "book",
    "candles",
    "trades",
    "mark_funding",
    "liquidations",
    "replay_candle_core",
    "data_quality_score",
    "weighted_exchange_controls",
    "liquidity_event_alerts",
    "signal_confluence_meter",
    "execution_simulator_panel",
    "confluence_score",
    "long_short_checklist",
    "entry_quality_meter",
    "regime_detector",
    "absorption_vs_breakout",
    "spoof_confidence_score",
    "mtf_alignment",
    "playbook_tags",
    "model_inputs_momentum",
    "model_inputs_volatility",
    "model_inputs_trend",
    "model_inputs_orderflow",
    "model_inputs_liquidity",
    "model_inputs_context",
    "main_chart",
    "orderbook_dominance",
    "live_ladder",
    "cvd_panel",
    "liquidity_area",
    "market_snapshot",
    "bias_meter",
    "positioning_panel",
    "spoofing_panel",
    "absorption_panel",
    "safety_panel",
)
MODE_IDS = ("live", "replay", "recording", "training", "bot")

DEFAULT_TOOL_MODE_MATRIX: dict[str, Any] = {
    "version": 1,
    "rows": {
        tool_id: {mode_id: True for mode_id in MODE_IDS}
        for tool_id in TOOL_IDS
    },
}

STREAM_TOOL_HINTS: tuple[tuple[str, str], ...] = (
    ("@depth", "book"),
    ("@bookticker", "book"),
    ("@aggtrade", "trades"),
    ("@trade", "trades"),
    ("@kline", "candles"),
    ("@markprice", "mark_funding"),
    ("@forceorder", "liquidations"),
)

FEATURE_COLUMNS_BY_TOOL: dict[str, set[str]] = {
    "book": {
        "spread",
        "book_bid_depth",
        "book_ask_depth",
        "book_imbalance_top10",
        "top10_bid_volume",
        "top10_ask_volume",
        "ladder_buy_pct",
        "ladder_sell_pct",
        "nearest_bid_wall_distance_pct",
        "nearest_ask_wall_distance_pct",
        "cancel_rate_orderbook",
        "wall_strength_bid",
        "wall_strength_ask",
        "wall_persistence_seconds",
        "spread_change_rate",
    },
    "candles": {
        "return_5s",
        "return_15s",
        "return_30s",
        "return_60s",
        "rolling_volatility_30s",
        "rolling_volatility_60s",
        "candle_range_pct",
        "atr_short",
        "ema_fast_distance_pct",
        "ema_slow_distance_pct",
        "trend_slope_short",
        "vwap_distance_pct",
        "hour_of_day_sin",
        "hour_of_day_cos",
        "session_asia_eu_us",
        "mid_price",
    },
    "trades": {
        "trade_rate_10s",
        "market_buy_volume_10s",
        "market_sell_volume_10s",
        "aggressive_buy_sell_delta",
        "volume_10s",
        "volume_30s",
        "relative_volume_ratio",
    },
    "mark_funding": {"funding_rate", "funding_rate_change", "open_interest_change_pct", "oi_velocity"},
    "liquidations": {"liq_events_60s"},
    "model_inputs_momentum": {"return_5s", "return_15s", "return_30s", "return_60s"},
    "model_inputs_volatility": {"rolling_volatility_30s", "rolling_volatility_60s", "candle_range_pct", "atr_short"},
    "model_inputs_trend": {"ema_fast_distance_pct", "ema_slow_distance_pct", "trend_slope_short", "vwap_distance_pct"},
    "model_inputs_orderflow": {"market_buy_volume_10s", "market_sell_volume_10s", "aggressive_buy_sell_delta", "trade_rate_10s"},
    "model_inputs_liquidity": {"wall_strength_bid", "wall_strength_ask", "spread_change_rate", "cancel_rate_orderbook"},
    "model_inputs_context": {"session_asia_eu_us", "open_interest_change_pct", "oi_velocity", "funding_rate_change"},
}


def normalize_tool_mode_matrix(raw: dict[str, Any] | None) -> dict[str, Any]:
    data = raw or {}
    rows_raw = data.get("rows", {}) if isinstance(data, dict) else {}
    rows: dict[str, dict[str, bool]] = {}
    for tool_id in TOOL_IDS:
        row_raw = rows_raw.get(tool_id, {}) if isinstance(rows_raw, dict) else {}
        row: dict[str, bool] = {}
        for mode_id in MODE_IDS:
            row[mode_id] = bool(row_raw.get(mode_id, True))
        rows[tool_id] = row
    return {"version": 1, "rows": rows}


def is_tool_enabled(matrix: dict[str, Any] | None, *, tool_id: str, mode_id: str) -> bool:
    if tool_id not in TOOL_IDS or mode_id not in MODE_IDS:
        return True
    normalized = normalize_tool_mode_matrix(matrix if isinstance(matrix, dict) else None)
    return bool(normalized["rows"].get(tool_id, {}).get(mode_id, True))


def event_stream_to_tool_id(stream: str | None, event_type: str | None = None) -> str | None:
    stream_l = str(stream or "").strip().lower()
    for needle, tool_id in STREAM_TOOL_HINTS:
        if needle in stream_l:
            return tool_id
    et = str(event_type or "").strip().lower()
    if et in {"depthupdate", "bookticker", "external_depth"}:
        return "book"
    if et in {"aggtrade", "trade", "external_trade_batch"}:
        return "trades"
    if et in {"kline", "external_ohlcv_batch"}:
        return "candles"
    if et in {"markpriceupdate", "markprice", "external_mark_funding"}:
        return "mark_funding"
    if et in {"forceorder", "external_liquidation_batch"}:
        return "liquidations"
    return None
