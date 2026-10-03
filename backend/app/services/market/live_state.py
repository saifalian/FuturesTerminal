from __future__ import annotations

import asyncio
import math
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.core.time_utils import utc_now_iso
from app.services.binance.depth_sync import DepthSync
from app.services.binance.rest_client import BinanceRestClient
from app.services.market.heatmap_frame_builder import HeatmapFrameBuilder
from app.services.market.heatmap_store import HeatmapStore
from app.services.market.ladder_snapshot_builder import LadderSnapshotBuilder
from app.services.market.price_bucketizer import PriceBucketizer


@dataclass(slots=True)
class LiveTerminalState:
    symbol: str
    rest_client: BinanceRestClient
    tick_size: float = 0.1
    ticks_per_bucket: int = 2
    depth_limit: int = 1000
    heatmap_time_bucket_ms: int = 500
    heatmap_max_columns: int = 14_400
    heatmap_bootstrap_columns: int = 700
    depth_sync: DepthSync = field(default_factory=DepthSync)
    stream_counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    event_count: int = 0
    best_bid: float = 0.0
    best_ask: float = 0.0
    mark_price: float = 0.0
    funding_rate: float = 0.0
    last_trade_price: float = 0.0
    last_trade_qty: float = 0.0
    last_trade_side: str = ""
    last_kline_close: float = 0.0
    last_kline_interval: str = "1m"
    last_liquidation_side: str = ""
    last_liquidation_qty: float = 0.0
    last_update_id: int = 0
    book_synced: bool = False
    sync_failures: int = 0
    _sync_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _recent_trade_times: deque[float] = field(default_factory=lambda: deque(maxlen=800))
    _recent_liq_times: deque[float] = field(default_factory=lambda: deque(maxlen=400))
    _last_snapshot_ts: float = 0.0
    _snapshot_interval_seconds: float = 0.5
    _last_heatmap_emit_ms: int = 0
    _last_resync_attempt_ts: float = 0.0
    _resync_cooldown_seconds: float = 1.5
    _resync_task: asyncio.Task[None] | None = None
    _resync_attempts: int = 0
    _external_trade_cursor_ms: dict[str, int] = field(default_factory=dict)
    _external_liq_cursor_ms: dict[str, int] = field(default_factory=dict)
    _external_kline_cursor_ms: dict[str, int] = field(default_factory=dict)
    _external_kline_rows: dict[int, dict[str, tuple[float, float, float, float]]] = field(default_factory=dict)
    _external_mark_data: dict[str, tuple[float, float, float]] = field(default_factory=dict)
    _trade_state_by_exchange: dict[str, tuple[float, float, str]] = field(default_factory=dict)
    _liq_state_by_exchange: dict[str, tuple[str, float]] = field(default_factory=dict)
    _recent_trade_times_by_exchange: dict[str, deque[float]] = field(default_factory=dict)
    _recent_liq_times_by_exchange: dict[str, deque[float]] = field(default_factory=dict)
    _recent_buy_qty: deque[tuple[float, float]] = field(default_factory=lambda: deque(maxlen=2000))
    _recent_sell_qty: deque[tuple[float, float]] = field(default_factory=lambda: deque(maxlen=2000))
    _recent_trade_notional_qty: deque[tuple[float, float, float]] = field(default_factory=lambda: deque(maxlen=3000))
    _mid_price_history: deque[tuple[float, float]] = field(default_factory=lambda: deque(maxlen=4000))
    _oi_history: deque[tuple[float, float]] = field(default_factory=lambda: deque(maxlen=2000))
    _spread_history: deque[tuple[float, float]] = field(default_factory=lambda: deque(maxlen=2000))
    _funding_history: deque[tuple[float, float]] = field(default_factory=lambda: deque(maxlen=2000))
    _candle_ranges: deque[float] = field(default_factory=lambda: deque(maxlen=30))
    _active_kline_ts_ms: int = 0
    _active_kline_open: float = 0.0
    _active_kline_high: float = 0.0
    _active_kline_low: float = 0.0
    open_interest: float = 0.0
    _enabled_exchanges: set[str] | None = None
    ladder_base_rows: int = 1200
    ladder_range_multiplier: int = 1
    price_bucketizer: PriceBucketizer = field(init=False)
    heatmap_store: HeatmapStore = field(init=False)
    heatmap_frame_builder: HeatmapFrameBuilder = field(init=False)
    ladder_snapshot_builder: LadderSnapshotBuilder = field(init=False)
    latest_ladder_snapshot: dict[str, Any] | None = None
    external_books: dict[str, dict[str, dict[float, float]]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.price_bucketizer = PriceBucketizer(self.tick_size, self.ticks_per_bucket)
        self.heatmap_store = HeatmapStore(self.heatmap_max_columns)
        self.heatmap_frame_builder = HeatmapFrameBuilder(self.price_bucketizer)
        self.ladder_snapshot_builder = LadderSnapshotBuilder(
            self.price_bucketizer,
            rows_count=max(120, self.ladder_base_rows * self.ladder_range_multiplier),
            use_tick_size=True,
        )
        self.symbol = self.symbol.upper()

    def configure_symbol(self, symbol: str, tick_size: float) -> None:
        self.tick_size = tick_size if tick_size > 0 else self.tick_size
        self.price_bucketizer.update_tick_size(self.tick_size)
        self.reset_for_symbol(symbol)

    def reset_for_symbol(self, symbol: str) -> None:
        self.symbol = symbol.upper()
        self.depth_sync.reset()
        self.stream_counts.clear()
        self.event_count = 0
        self.best_bid = 0.0
        self.best_ask = 0.0
        self.mark_price = 0.0
        self.funding_rate = 0.0
        self.open_interest = 0.0
        self.last_trade_price = 0.0
        self.last_trade_qty = 0.0
        self.last_trade_side = ""
        self.last_kline_close = 0.0
        self.last_kline_interval = "1m"
        self.last_liquidation_side = ""
        self.last_liquidation_qty = 0.0
        self.last_update_id = 0
        self.book_synced = False
        self.sync_failures = 0
        self._recent_trade_times.clear()
        self._recent_liq_times.clear()
        self._recent_buy_qty.clear()
        self._recent_sell_qty.clear()
        self._recent_trade_notional_qty.clear()
        self._mid_price_history.clear()
        self._oi_history.clear()
        self._spread_history.clear()
        self._funding_history.clear()
        self._candle_ranges.clear()
        self._active_kline_ts_ms = 0
        self._active_kline_open = 0.0
        self._active_kline_high = 0.0
        self._active_kline_low = 0.0
        self._last_snapshot_ts = 0.0
        self._last_heatmap_emit_ms = 0
        self._last_resync_attempt_ts = 0.0
        self._resync_attempts = 0
        if self._resync_task and not self._resync_task.done():
            self._resync_task.cancel()
        self._resync_task = None
        self.heatmap_store.reset()
        self.latest_ladder_snapshot = None
        self.external_books.clear()
        self._external_trade_cursor_ms.clear()
        self._external_liq_cursor_ms.clear()
        self._external_kline_cursor_ms.clear()
        self._external_kline_rows.clear()
        self._external_mark_data.clear()
        self._trade_state_by_exchange.clear()
        self._liq_state_by_exchange.clear()
        self._recent_trade_times_by_exchange.clear()
        self._recent_liq_times_by_exchange.clear()

    def _append_mid_sample(self, now: float, mid_price: float) -> None:
        if mid_price <= 0:
            return
        self._mid_price_history.append((now, float(mid_price)))
        while self._mid_price_history and now - self._mid_price_history[0][0] > 65.0:
            self._mid_price_history.popleft()

    def _append_open_interest(self, now: float, oi_value: float) -> None:
        if oi_value <= 0:
            return
        self.open_interest = oi_value
        self._oi_history.append((now, float(oi_value)))
        while self._oi_history and now - self._oi_history[0][0] > 125.0:
            self._oi_history.popleft()

    def _update_active_kline(self, ts_ms: int, o: float, h: float, l: float) -> None:
        if ts_ms <= 0:
            return
        if self._active_kline_ts_ms and ts_ms != self._active_kline_ts_ms:
            if self._active_kline_high > 0 and self._active_kline_low > 0:
                self._candle_ranges.append(max(0.0, self._active_kline_high - self._active_kline_low))
            self._active_kline_open = 0.0
            self._active_kline_high = 0.0
            self._active_kline_low = 0.0
        self._active_kline_ts_ms = ts_ms
        if o > 0 and self._active_kline_open <= 0:
            self._active_kline_open = o
        if h > 0:
            self._active_kline_high = h if self._active_kline_high <= 0 else max(self._active_kline_high, h)
        if l > 0:
            self._active_kline_low = l if self._active_kline_low <= 0 else min(self._active_kline_low, l)

    def _phase1_features(self, now: float, mid_price: float) -> dict[str, float]:
        def _mid_ago(seconds: float) -> float | None:
            for ts, value in reversed(self._mid_price_history):
                if now - ts >= seconds:
                    return value
            return None

        mid_5 = _mid_ago(5.0)
        mid_15 = _mid_ago(15.0)
        mid_30 = _mid_ago(30.0)
        mid_60 = _mid_ago(60.0)
        ret_5 = ((mid_price - mid_5) / mid_5) if mid_5 and mid_5 > 0 and mid_price > 0 else 0.0
        ret_15 = ((mid_price - mid_15) / mid_15) if mid_15 and mid_15 > 0 and mid_price > 0 else 0.0
        ret_30 = ((mid_price - mid_30) / mid_30) if mid_30 and mid_30 > 0 and mid_price > 0 else 0.0
        ret_60 = ((mid_price - mid_60) / mid_60) if mid_60 and mid_60 > 0 and mid_price > 0 else 0.0

        rets_30: list[float] = []
        prev = None
        for ts, value in self._mid_price_history:
            if now - ts > 30.0:
                continue
            if prev and prev > 0 and value > 0:
                rets_30.append((value - prev) / prev)
            prev = value
        if len(rets_30) >= 2:
            mean = sum(rets_30) / len(rets_30)
            var = sum((r - mean) ** 2 for r in rets_30) / max(1, len(rets_30) - 1)
            vol_30 = var**0.5
        else:
            vol_30 = 0.0
        rets_60: list[float] = []
        prev60 = None
        for ts, value in self._mid_price_history:
            if now - ts > 60.0:
                continue
            if prev60 and prev60 > 0 and value > 0:
                rets_60.append((value - prev60) / prev60)
            prev60 = value
        if len(rets_60) >= 2:
            mean60 = sum(rets_60) / len(rets_60)
            var60 = sum((r - mean60) ** 2 for r in rets_60) / max(1, len(rets_60) - 1)
            vol_60 = var60**0.5
        else:
            vol_60 = 0.0

        while self._recent_buy_qty and now - self._recent_buy_qty[0][0] > 10.0:
            self._recent_buy_qty.popleft()
        while self._recent_sell_qty and now - self._recent_sell_qty[0][0] > 10.0:
            self._recent_sell_qty.popleft()
        while self._recent_trade_notional_qty and now - self._recent_trade_notional_qty[0][0] > 10.0:
            self._recent_trade_notional_qty.popleft()
        volume_30_items = [item for item in self._recent_trade_notional_qty if now - item[0] <= 30.0]
        buy_10 = sum(item[1] for item in self._recent_buy_qty)
        sell_10 = sum(item[1] for item in self._recent_sell_qty)
        vol_10 = max(0.0, buy_10 + sell_10)
        vol_30_qty = max(0.0, sum(item[2] for item in volume_30_items))
        vwap_notional = sum(item[1] for item in self._recent_trade_notional_qty)
        vwap_qty = sum(item[2] for item in self._recent_trade_notional_qty)
        vwap_10 = (vwap_notional / vwap_qty) if vwap_qty > 0 else mid_price
        vwap_distance_pct = ((mid_price - vwap_10) / vwap_10) if vwap_10 > 0 and mid_price > 0 else 0.0
        aggressive_delta = ((buy_10 - sell_10) / vol_10) if vol_10 > 0 else 0.0
        relative_volume_ratio = (vol_10 / (vol_30_qty / 3.0)) if vol_30_qty > 0 else 0.0

        ema_fast = mid_price
        ema_slow = mid_price
        alpha_fast = 2.0 / 9.0
        alpha_slow = 2.0 / 22.0
        for ts, value in self._mid_price_history:
            if now - ts > 60.0:
                continue
            ema_fast = alpha_fast * value + (1.0 - alpha_fast) * ema_fast
            ema_slow = alpha_slow * value + (1.0 - alpha_slow) * ema_slow
        ema_fast_distance = ((mid_price - ema_fast) / ema_fast) if ema_fast > 0 and mid_price > 0 else 0.0
        ema_slow_distance = ((mid_price - ema_slow) / ema_slow) if ema_slow > 0 and mid_price > 0 else 0.0
        trend_anchor = None
        for ts, value in reversed(self._mid_price_history):
            if now - ts >= 20.0:
                trend_anchor = value
                break
        trend_slope_short = ((mid_price - trend_anchor) / trend_anchor) if trend_anchor and trend_anchor > 0 and mid_price > 0 else 0.0

        candle_range_pct = (
            (self._active_kline_high - self._active_kline_low) / self._active_kline_open
            if self._active_kline_open > 0 and self._active_kline_high > 0 and self._active_kline_low > 0
            else 0.0
        )
        atr_short_raw = (sum(self._candle_ranges) / len(self._candle_ranges)) if self._candle_ranges else max(0.0, self._active_kline_high - self._active_kline_low)
        atr_short = (atr_short_raw / mid_price) if mid_price > 0 else 0.0

        while self._spread_history and now - self._spread_history[0][0] > 30.0:
            self._spread_history.popleft()
        spread_ref = None
        for ts, value in reversed(self._spread_history):
            if now - ts >= 5.0:
                spread_ref = value
                break
        spread_now = self._spread_history[-1][1] if self._spread_history else 0.0
        spread_change_rate = ((spread_now - spread_ref) / spread_ref) if spread_ref and spread_ref > 0 else 0.0
        now_dt = datetime.now(timezone.utc)
        hour_float = now_dt.hour + (now_dt.minute / 60.0) + (now_dt.second / 3600.0)
        hour_angle = (hour_float / 24.0) * (2.0 * math.pi)
        hour_of_day_sin = math.sin(hour_angle)
        hour_of_day_cos = math.cos(hour_angle)
        if now_dt.hour < 8:
            session_code = 0.0
        elif now_dt.hour < 16:
            session_code = 1.0
        else:
            session_code = 2.0
        oi_prev = None
        for ts, value in reversed(self._oi_history):
            if now - ts >= 10.0:
                oi_prev = value
                break
        oi_velocity = ((self.open_interest - oi_prev) / oi_prev) if oi_prev and oi_prev > 0 else 0.0
        while self._funding_history and now - self._funding_history[0][0] > 120.0:
            self._funding_history.popleft()
        funding_prev = None
        for ts, value in reversed(self._funding_history):
            if now - ts >= 60.0:
                funding_prev = value
                break
        funding_rate_change = (self.funding_rate - funding_prev) if funding_prev is not None else 0.0

        oi_ref = None
        for ts, value in reversed(self._oi_history):
            if now - ts >= 60.0:
                oi_ref = value
                break
        oi_change_pct = ((self.open_interest - oi_ref) / oi_ref) if oi_ref and oi_ref > 0 else 0.0

        return {
            "return_5s": float(ret_5),
            "return_15s": float(ret_15),
            "return_30s": float(ret_30),
            "return_60s": float(ret_60),
            "rolling_volatility_30s": float(vol_30),
            "rolling_volatility_60s": float(vol_60),
            "candle_range_pct": float(candle_range_pct),
            "atr_short": float(atr_short),
            "ema_fast_distance_pct": float(ema_fast_distance),
            "ema_slow_distance_pct": float(ema_slow_distance),
            "trend_slope_short": float(trend_slope_short),
            "vwap_distance_pct": float(vwap_distance_pct),
            "market_buy_volume_10s": float(max(0.0, buy_10)),
            "market_sell_volume_10s": float(max(0.0, sell_10)),
            "aggressive_buy_sell_delta": float(aggressive_delta),
            "cancel_rate_orderbook": 0.0,
            "wall_strength_bid": 0.0,
            "wall_strength_ask": 0.0,
            "wall_persistence_seconds": 0.0,
            "spread_change_rate": float(spread_change_rate),
            "volume_10s": float(vol_10),
            "volume_30s": float(vol_30_qty),
            "relative_volume_ratio": float(relative_volume_ratio),
            "hour_of_day_sin": float(hour_of_day_sin),
            "hour_of_day_cos": float(hour_of_day_cos),
            "session_asia_eu_us": float(session_code),
            "oi_velocity": float(oi_velocity),
            "funding_rate_change": float(funding_rate_change),
            "open_interest_change_pct": float(oi_change_pct),
        }

    def is_exchange_enabled(self, exchange_id: str) -> bool:
        if self._enabled_exchanges is None:
            return True
        return exchange_id in self._enabled_exchanges

    def apply_enabled_exchanges(self, enabled_exchange_ids: set[str]) -> None:
        self._enabled_exchanges = set(enabled_exchange_ids)
        self.reset_for_symbol(self.symbol)

    def set_ladder_range_multiplier(self, multiplier: int) -> dict[str, int]:
        clamped = max(1, min(10, int(multiplier)))
        self.ladder_range_multiplier = clamped
        rows_count = max(120, int(self.ladder_base_rows * clamped))
        self.ladder_snapshot_builder.rows_count = rows_count
        self.latest_ladder_snapshot = None
        return {
            "range_multiplier": clamped,
            "rows_count": rows_count,
        }

    async def process_event(self, event: dict[str, Any]) -> None:
        if not self.is_exchange_enabled("binance"):
            return
        stream = str(event.get("stream", ""))
        payload = event.get("payload", {})
        if not isinstance(payload, dict):
            return

        self.event_count += 1
        self.stream_counts[stream] = self.stream_counts.get(stream, 0) + 1

        if "@depth" in stream:
            await self._process_depth(payload)
            return
        if "@bookTicker" in stream:
            self.best_bid = float(payload.get("b", self.best_bid) or self.best_bid)
            self.best_ask = float(payload.get("a", self.best_ask) or self.best_ask)
            return
        if "@aggTrade" in stream:
            self.last_trade_price = float(payload.get("p", self.last_trade_price) or self.last_trade_price)
            self.last_trade_qty = float(payload.get("q", self.last_trade_qty) or self.last_trade_qty)
            is_sell_aggressor = bool(payload.get("m", False))
            self.last_trade_side = "SELL" if is_sell_aggressor else "BUY"
            now = time.monotonic()
            self._recent_trade_times.append(now)
            if self.last_trade_qty > 0:
                if self.last_trade_side == "BUY":
                    self._recent_buy_qty.append((now, float(self.last_trade_qty)))
                elif self.last_trade_side == "SELL":
                    self._recent_sell_qty.append((now, float(self.last_trade_qty)))
                if self.last_trade_price > 0:
                    self._recent_trade_notional_qty.append(
                        (now, float(self.last_trade_price) * float(self.last_trade_qty), float(self.last_trade_qty))
                    )
            side = "SELL" if is_sell_aggressor else "BUY"
            self._trade_state_by_exchange["binance"] = (self.last_trade_price, self.last_trade_qty, side)
            trade_q = self._recent_trade_times_by_exchange.setdefault("binance", deque(maxlen=800))
            trade_q.append(now)
            return
        if "@markPrice" in stream:
            self.mark_price = float(payload.get("p", self.mark_price) or self.mark_price)
            self.funding_rate = float(payload.get("r", self.funding_rate) or self.funding_rate)
            self._funding_history.append((time.monotonic(), self.funding_rate))
            oi_raw = payload.get("open_interest")
            if oi_raw is None:
                oi_raw = payload.get("openInterest")
            if oi_raw is None:
                oi_raw = payload.get("oi")
            try:
                oi_value = float(oi_raw or 0.0)
            except Exception:
                oi_value = 0.0
            if oi_value > 0:
                self._append_open_interest(time.monotonic(), oi_value)
            self._external_mark_data["binance"] = (self.mark_price, self.funding_rate, time.monotonic())
            return
        if "@kline_" in stream:
            kline = payload.get("k", {})
            if isinstance(kline, dict):
                self.last_kline_close = float(kline.get("c", self.last_kline_close) or self.last_kline_close)
                self.last_kline_interval = str(kline.get("i", self.last_kline_interval))
                self._update_active_kline(
                    int(kline.get("t") or 0),
                    float(kline.get("o") or 0.0),
                    float(kline.get("h") or 0.0),
                    float(kline.get("l") or 0.0),
                )
            return
        if "@forceOrder" in stream:
            order = payload.get("o", {})
            if isinstance(order, dict):
                self.last_liquidation_side = str(order.get("S", self.last_liquidation_side))
                self.last_liquidation_qty = float(order.get("q", self.last_liquidation_qty) or self.last_liquidation_qty)
            self._recent_liq_times.append(time.monotonic())
            self._liq_state_by_exchange["binance"] = (self.last_liquidation_side, self.last_liquidation_qty)
            liq_q = self._recent_liq_times_by_exchange.setdefault("binance", deque(maxlen=400))
            liq_q.append(time.monotonic())

    async def _process_depth(self, payload: dict[str, Any]) -> None:
        if not self.depth_sync.synced:
            self.depth_sync.buffer_event(payload)
            self._schedule_resync_if_needed()
            return

        synced = self.depth_sync.apply_live_event(payload)
        if not synced:
            self.sync_failures += 1
            self.depth_sync.buffer_event(payload)
            self._schedule_resync_if_needed()
            return

        self._update_top_of_book_from_depth()

    def _schedule_resync_if_needed(self) -> None:
        if self.depth_sync.synced:
            self.book_synced = True
            return
        now = time.monotonic()
        if self._sync_lock.locked():
            return
        if self._resync_task and not self._resync_task.done():
            return
        cooldown = min(12.0, self._resync_cooldown_seconds * max(1, self._resync_attempts + 1))
        if now - self._last_resync_attempt_ts < cooldown:
            return
        self._last_resync_attempt_ts = now
        self._resync_task = asyncio.create_task(self._resync_from_snapshot_task(), name=f"depth-resync-{self.symbol.lower()}")

    async def _resync_from_snapshot_task(self) -> None:
        try:
            await self._resync_from_snapshot()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.book_synced = False
        finally:
            self._resync_task = None

    async def _resync_from_snapshot(self) -> bool:
        async with self._sync_lock:
            snapshot = await self.rest_client.depth_snapshot(self.symbol, limit=self.depth_limit)
            synced = self.depth_sync.sync_from_snapshot(snapshot)
            if not synced:
                # Stability-first: if bridge is not found quickly, force-sync to
                # avoid endless snapshot loops and keep the terminal updating.
                self._resync_attempts += 1
                self.depth_sync.force_sync_from_snapshot(snapshot)
                synced = True
                self._resync_attempts = 0
            else:
                self._resync_attempts = 0
            self.book_synced = synced
            self.last_update_id = self.depth_sync.book.last_update_id
            if synced:
                self._update_top_of_book_from_depth()
            return synced

    def _update_top_of_book_from_depth(self) -> None:
        merged_bids, merged_asks = self._merged_books()
        bid = max(merged_bids.items(), key=lambda x: x[0]) if merged_bids else None
        ask = min(merged_asks.items(), key=lambda x: x[0]) if merged_asks else None
        if bid:
            self.best_bid = float(bid[0])
        else:
            self.best_bid = 0.0
        if ask:
            self.best_ask = float(ask[0])
        else:
            self.best_ask = 0.0
        self.last_update_id = self.depth_sync.book.last_update_id

    def set_external_book(self, exchange_id: str, bids: list[tuple[float, float]], asks: list[tuple[float, float]]) -> None:
        if not self.is_exchange_enabled(exchange_id):
            return
        bid_map = {float(price): float(qty) for price, qty in bids if qty > 0}
        ask_map = {float(price): float(qty) for price, qty in asks if qty > 0}
        self.external_books[exchange_id] = {"bids": bid_map, "asks": ask_map}
        self._update_top_of_book_from_depth()

    def _merged_books(self) -> tuple[dict[float, float], dict[float, float]]:
        bids: dict[float, float] = {}
        asks: dict[float, float] = {}
        if self.is_exchange_enabled("binance"):
            bids.update(self.depth_sync.book.bids)
            asks.update(self.depth_sync.book.asks)
        for exchange_id, book in self.external_books.items():
            if not self.is_exchange_enabled(exchange_id):
                continue
            for price, qty in book.get("bids", {}).items():
                if qty <= 0:
                    continue
                bids[price] = bids.get(price, 0.0) + qty
            for price, qty in book.get("asks", {}).items():
                if qty <= 0:
                    continue
                asks[price] = asks.get(price, 0.0) + qty
        return bids, asks

    def _reference_price(self) -> float:
        if self.mark_price > 0:
            return self.mark_price
        if self.best_bid > 0 and self.best_ask > 0:
            return (self.best_bid + self.best_ask) / 2
        if self.last_trade_price > 0:
            return self.last_trade_price
        if self.best_bid > 0:
            return self.best_bid
        if self.best_ask > 0:
            return self.best_ask
        return 0.0

    def ingest_external_trades(self, exchange_id: str, trades: list[dict[str, Any]]) -> None:
        if not self.is_exchange_enabled(exchange_id):
            return
        if not trades:
            return
        now = time.monotonic()
        cursor = self._external_trade_cursor_ms.get(exchange_id, 0)
        new_count = 0
        for trade in sorted(trades, key=lambda item: int(item.get("ts_ms", 0))):
            try:
                ts_ms = int(trade.get("ts_ms", 0))
            except Exception:
                continue
            if ts_ms <= cursor:
                continue
            cursor = ts_ms
            price = float(trade.get("price", 0.0) or 0.0)
            qty = float(trade.get("qty", 0.0) or 0.0)
            side = str(trade.get("side", "")).upper()
            if price > 0:
                self.last_trade_price = price
            if qty > 0:
                self.last_trade_qty = qty
            if side in {"BUY", "SELL"}:
                self.last_trade_side = side
            self._trade_state_by_exchange[exchange_id] = (price, qty, side if side in {"BUY", "SELL"} else "")
            self._recent_trade_times.append(now)
            if qty > 0 and side == "BUY":
                self._recent_buy_qty.append((now, qty))
            elif qty > 0 and side == "SELL":
                self._recent_sell_qty.append((now, qty))
            if price > 0 and qty > 0:
                self._recent_trade_notional_qty.append((now, price * qty, qty))
            trade_q = self._recent_trade_times_by_exchange.setdefault(exchange_id, deque(maxlen=800))
            trade_q.append(now)
            new_count += 1
            if new_count >= 120:
                break
        self._external_trade_cursor_ms[exchange_id] = cursor

    def ingest_external_ohlcv(self, exchange_id: str, interval: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not self.is_exchange_enabled(exchange_id):
            return []
        if interval != "1m" or not rows:
            return []
        cursor_key = f"{exchange_id}:{interval}"
        prev_cursor = self._external_kline_cursor_ms.get(cursor_key, 0)
        cursor = prev_cursor
        touched_ts: set[int] = set()
        for row in sorted(rows, key=lambda item: int(item.get("ts_ms", 0))):
            try:
                ts_ms = int(row.get("ts_ms", 0))
                o = float(row.get("open", 0.0) or 0.0)
                h = float(row.get("high", 0.0) or 0.0)
                l = float(row.get("low", 0.0) or 0.0)
                c = float(row.get("close", 0.0) or 0.0)
            except Exception:
                continue
            # Keep current-minute updates flowing; ignore only strictly older rows.
            if ts_ms < cursor:
                continue
            if ts_ms > cursor:
                cursor = ts_ms
            bucket = self._external_kline_rows.setdefault(ts_ms, {})
            bucket[exchange_id] = (o, h, l, c)
            touched_ts.add(ts_ms)
            self._update_active_kline(ts_ms, o, h, l)

        if cursor > 0:
            self._external_kline_cursor_ms[cursor_key] = cursor
        if not touched_ts:
            return []

        def _build_kline_event(ts_ms: int, is_closed: bool) -> dict[str, Any] | None:
            points = self._external_kline_rows.get(ts_ms, {})
            if not points:
                return None
            opens = [item[0] for item in points.values()]
            highs = [item[1] for item in points.values()]
            lows = [item[2] for item in points.values()]
            closes = [item[3] for item in points.values()]
            if not opens or not highs or not lows or not closes:
                return None

            open_price = sum(opens) / len(opens)
            high_price = max(highs)
            low_price = min(lows)
            close_price = sum(closes) / len(closes)
            self.last_kline_close = close_price
            self.last_kline_interval = interval
            return {
                "type": "market_event",
                "symbol": self.symbol,
                "stream": "aggregate@kline_1m",
                "event_type": "kline",
                "received_at": utc_now_iso(),
                "payload": {
                    "k": {
                        "s": self.symbol,
                        "t": ts_ms,
                        "T": ts_ms + 60_000 - 1,
                        "i": "1m",
                        "o": str(open_price),
                        "h": str(high_price),
                        "l": str(low_price),
                        "c": str(close_price),
                        "x": bool(is_closed),
                    }
                },
            }

        events: list[dict[str, Any]] = []

        # Emit the just-closed minute once we roll into a newer minute.
        if prev_cursor > 0 and cursor > prev_cursor:
            closed_event = _build_kline_event(prev_cursor, True)
            if closed_event:
                events.append(closed_event)

        # Emit current minute as in-progress so chart updates continuously.
        latest_ts = max(touched_ts)
        in_progress_event = _build_kline_event(latest_ts, False)
        if in_progress_event:
            events.append(in_progress_event)

        min_alive_ts = latest_ts - (6 * 60_000)
        stale_keys = [bucket_ts for bucket_ts in self._external_kline_rows.keys() if bucket_ts < min_alive_ts]
        for bucket_ts in stale_keys:
            self._external_kline_rows.pop(bucket_ts, None)

        return events

    def ingest_external_mark_funding(
        self,
        exchange_id: str,
        mark_price: float,
        funding_rate: float,
        open_interest: float | None = None,
    ) -> None:
        if not self.is_exchange_enabled(exchange_id):
            return
        now = time.monotonic()
        self._external_mark_data[exchange_id] = (mark_price, funding_rate, now)
        alive = [
            (ex_id, values)
            for ex_id, values in self._external_mark_data.items()
            if self.is_exchange_enabled(ex_id) and now - values[2] <= 30.0
        ]
        if not alive:
            return
        marks = [item[1][0] for item in alive if item[1][0] > 0]
        fundings = [item[1][1] for item in alive]
        if marks:
            self.mark_price = sum(marks) / len(marks)
        if fundings:
            self.funding_rate = sum(fundings) / len(fundings)
            self._funding_history.append((now, self.funding_rate))
        if open_interest is not None:
            try:
                oi_value = float(open_interest or 0.0)
            except Exception:
                oi_value = 0.0
            if oi_value > 0:
                self._append_open_interest(now, oi_value)
        if self.mark_price > 0:
            self._append_mid_sample(now, self.mark_price)

    def ingest_external_liquidations(self, exchange_id: str, liquidations: list[dict[str, Any]]) -> None:
        if not self.is_exchange_enabled(exchange_id):
            return
        if not liquidations:
            return
        now = time.monotonic()
        cursor = self._external_liq_cursor_ms.get(exchange_id, 0)
        new_count = 0
        for liq in sorted(liquidations, key=lambda item: int(item.get("ts_ms", 0))):
            try:
                ts_ms = int(liq.get("ts_ms", 0))
            except Exception:
                continue
            if ts_ms <= cursor:
                continue
            cursor = ts_ms
            qty = float(liq.get("qty", 0.0) or 0.0)
            side = str(liq.get("side", "")).upper()
            if qty > 0:
                self.last_liquidation_qty = qty
            if side in {"BUY", "SELL"}:
                self.last_liquidation_side = side
            self._liq_state_by_exchange[exchange_id] = (side if side in {"BUY", "SELL"} else "", qty)
            self._recent_liq_times.append(now)
            liq_q = self._recent_liq_times_by_exchange.setdefault(exchange_id, deque(maxlen=400))
            liq_q.append(now)
            new_count += 1
            if new_count >= 120:
                break
        self._external_liq_cursor_ms[exchange_id] = cursor

    def _book_for_exchange(self, exchange_id: str) -> tuple[dict[float, float], dict[float, float]]:
        if exchange_id == "binance":
            return (dict(self.depth_sync.book.bids), dict(self.depth_sync.book.asks))
        book = self.external_books.get(exchange_id) or {}
        return (dict(book.get("bids", {})), dict(book.get("asks", {})))

    def _build_ml_snapshot_payload(
        self,
        *,
        tab_id: str,
        enabled_exchange_ids: list[str],
        connected_exchanges: int,
        bids: dict[float, float],
        asks: dict[float, float],
        mark_price: float,
        funding_rate: float,
        last_trade_price: float,
        last_trade_qty: float,
        last_trade_side: str,
        liq_events_60s: float,
        last_liquidation_qty: float,
        last_liquidation_side: str,
    ) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        bid_items = sorted(bids.items(), key=lambda item: item[0], reverse=True)
        ask_items = sorted(asks.items(), key=lambda item: item[0])
        top10_bid_vol = sum(float(qty) for _, qty in bid_items[:10])
        top10_ask_vol = sum(float(qty) for _, qty in ask_items[:10])
        total_bid_depth = sum(float(qty) for _, qty in bid_items)
        total_ask_depth = sum(float(qty) for _, qty in ask_items)
        imbalance = 0.0
        denominator = top10_bid_vol + top10_ask_vol
        if denominator > 0:
            imbalance = (top10_bid_vol - top10_ask_vol) / denominator
        best_bid = float(bid_items[0][0]) if bid_items else 0.0
        best_ask = float(ask_items[0][0]) if ask_items else 0.0
        mid_price = 0.0
        if best_bid > 0 and best_ask > 0:
            mid_price = (best_bid + best_ask) / 2
        elif last_trade_price > 0:
            mid_price = last_trade_price
        elif mark_price > 0:
            mid_price = mark_price
        self._append_mid_sample(time.monotonic(), mid_price)

        trade_rate_10s = max(0.0, float(liq_events_60s) * 0.0)  # placeholder to initialize below
        # trade_rate is computed by caller-specific queues; overwritten by caller.
        trade_rate_10s = 0.0

        ladder_buy_liq = sum(float(qty) for _, qty in bid_items[:20])
        ladder_sell_liq = sum(float(qty) for _, qty in ask_items[:20])
        ladder_total = ladder_buy_liq + ladder_sell_liq
        ladder_buy_pct = (ladder_buy_liq / ladder_total * 100.0) if ladder_total > 0 else 0.0
        ladder_sell_pct = (ladder_sell_liq / ladder_total * 100.0) if ladder_total > 0 else 0.0

        nearest_bid_wall_distance_pct = 0.0
        nearest_ask_wall_distance_pct = 0.0
        if mid_price > 0:
            bid_wall_levels = [price for price, qty in bid_items[:50] if qty >= max(1.0, top10_bid_vol / 25 if top10_bid_vol > 0 else 1.0)]
            ask_wall_levels = [price for price, qty in ask_items[:50] if qty >= max(1.0, top10_ask_vol / 25 if top10_ask_vol > 0 else 1.0)]
            if bid_wall_levels:
                nearest_bid = max(bid_wall_levels)
                nearest_bid_wall_distance_pct = abs((mid_price - nearest_bid) / mid_price) * 100.0
            if ask_wall_levels:
                nearest_ask = min(ask_wall_levels)
                nearest_ask_wall_distance_pct = abs((nearest_ask - mid_price) / mid_price) * 100.0
        spread_now = (best_ask - best_bid) if best_bid and best_ask else 0.0
        self._spread_history.append((time.monotonic(), spread_now))
        wall_total = top10_bid_vol + top10_ask_vol
        wall_strength_bid = (top10_bid_vol / wall_total) if wall_total > 0 else 0.0
        wall_strength_ask = (top10_ask_vol / wall_total) if wall_total > 0 else 0.0

        phase1 = self._phase1_features(time.monotonic(), mid_price)
        return {
            "schema_version": "v2",
            "ts_ms": now_ms,
            "tab_id": tab_id,
            "pair_symbol": self.symbol,
            "enabled_exchange_ids": enabled_exchange_ids,
            "connected_exchanges": connected_exchanges,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread": (best_ask - best_bid) if best_bid and best_ask else 0.0,
            "mid_price": mid_price,
            "book_bid_depth": total_bid_depth,
            "book_ask_depth": total_ask_depth,
            "book_imbalance_top10": imbalance,
            "top10_bid_volume": top10_bid_vol,
            "top10_ask_volume": top10_ask_vol,
            "trade_rate_10s": trade_rate_10s,
            "last_trade_price": last_trade_price,
            "last_trade_qty": last_trade_qty,
            "last_trade_side": last_trade_side,
            "liq_events_60s": liq_events_60s,
            "last_liquidation_qty": last_liquidation_qty,
            "last_liquidation_side": last_liquidation_side,
            "mark_price": mark_price,
            "funding_rate": funding_rate,
            "last_kline_close": self.last_kline_close,
            "last_kline_interval": self.last_kline_interval,
            "ladder_buy_liquidity": ladder_buy_liq,
            "ladder_sell_liquidity": ladder_sell_liq,
            "ladder_buy_pct": ladder_buy_pct,
            "ladder_sell_pct": ladder_sell_pct,
            "nearest_bid_wall_distance_pct": nearest_bid_wall_distance_pct,
            "nearest_ask_wall_distance_pct": nearest_ask_wall_distance_pct,
            "return_5s": phase1["return_5s"],
            "return_15s": phase1["return_15s"],
            "return_30s": phase1["return_30s"],
            "return_60s": phase1["return_60s"],
            "rolling_volatility_30s": phase1["rolling_volatility_30s"],
            "rolling_volatility_60s": phase1["rolling_volatility_60s"],
            "candle_range_pct": phase1["candle_range_pct"],
            "atr_short": phase1["atr_short"],
            "ema_fast_distance_pct": phase1["ema_fast_distance_pct"],
            "ema_slow_distance_pct": phase1["ema_slow_distance_pct"],
            "trend_slope_short": phase1["trend_slope_short"],
            "vwap_distance_pct": phase1["vwap_distance_pct"],
            "market_buy_volume_10s": phase1["market_buy_volume_10s"],
            "market_sell_volume_10s": phase1["market_sell_volume_10s"],
            "aggressive_buy_sell_delta": phase1["aggressive_buy_sell_delta"],
            "cancel_rate_orderbook": phase1["cancel_rate_orderbook"],
            "wall_strength_bid": wall_strength_bid,
            "wall_strength_ask": wall_strength_ask,
            "wall_persistence_seconds": phase1["wall_persistence_seconds"],
            "spread_change_rate": phase1["spread_change_rate"],
            "volume_10s": phase1["volume_10s"],
            "volume_30s": phase1["volume_30s"],
            "relative_volume_ratio": phase1["relative_volume_ratio"],
            "hour_of_day_sin": phase1["hour_of_day_sin"],
            "hour_of_day_cos": phase1["hour_of_day_cos"],
            "session_asia_eu_us": phase1["session_asia_eu_us"],
            "oi_velocity": phase1["oi_velocity"],
            "funding_rate_change": phase1["funding_rate_change"],
            "open_interest_change_pct": phase1["open_interest_change_pct"],
            "book_synced": bool(self.book_synced),
            "sync_failures": int(self.sync_failures),
            "event_count": int(self.event_count),
            "last_update_id": int(self.last_update_id),
        }

    def should_emit_snapshot(self) -> bool:
        now = time.monotonic()
        if now - self._last_snapshot_ts >= self._snapshot_interval_seconds:
            self._last_snapshot_ts = now
            return True
        return False

    def _build_decision_layer(
        self,
        spread: float,
        trade_rate: float,
        liq_per_min: int,
        source_coverage: dict[str, Any] | None,
    ) -> dict[str, Any]:
        connected_exchanges = 0
        supported_exchanges = 1
        if isinstance(source_coverage, dict):
            connected_exchanges = int(source_coverage.get("connected_exchanges", 0) or 0)
            supported_exchanges = int(source_coverage.get("supported_exchanges", 1) or 1)

        connected_ratio = max(0.0, min(1.0, connected_exchanges / max(1, supported_exchanges)))
        sync_score = 1.0 if self.book_synced else 0.35
        spread_score = max(0.0, 1 - min(1.0, spread / 0.2))
        trade_score = min(1.0, trade_rate / 15.0)

        last_side = (self.last_trade_side or "").upper()
        long_score = min(1.0, 0.5 + trade_rate / 20.0) if last_side == "BUY" else 0.3
        short_score = min(1.0, 0.5 + trade_rate / 20.0) if last_side == "SELL" else 0.3
        confidence = min(1.0, trade_rate / 10.0)
        danger_score = min(1.0, liq_per_min / 20.0)
        confluence_score = round(
            (
                long_score * 0.24
                + short_score * 0.18
                + confidence * 0.28
                + trade_score * 0.15
                + spread_score * 0.15
            )
            * 100
        )
        data_quality_score = round((connected_ratio * 0.4 + sync_score * 0.35 + spread_score * 0.25) * 100)

        if liq_per_min >= 8:
            regime = "Liquidation-driven"
        elif trade_rate >= 12 and spread <= 0.05:
            regime = "Trend"
        elif trade_rate <= 4:
            regime = "Chop"
        else:
            regime = "Balanced"

        if spread <= 0.03 and confidence >= 0.6:
            entry_quality = "Premium"
        elif spread <= 0.08:
            entry_quality = "Neutral"
        else:
            entry_quality = "Chase"

        confidence_pct = round(confidence * 100)
        danger_pct = round(danger_score * 100)
        spoof_confidence = max(0, 100 - self.sync_failures * 5)
        fill_probability_pct = round((trade_score * 0.5 + spread_score * 0.5) * 100)
        breakout_pressure_pct = round(trade_score * 100)

        if regime == "Trend":
            active_playbook = "Breakout Pullback"
        elif regime == "Liquidation-driven":
            active_playbook = "Sweep/Fade"
        else:
            active_playbook = "Range Rotation"

        return {
            "data_quality_score": {
                "score": data_quality_score,
                "connected_exchanges": connected_exchanges,
                "supported_exchanges": supported_exchanges,
                "book_sync_status": "Healthy" if self.book_synced else "Resyncing",
                "note": "Live trust based on feed coverage + sync + spread health.",
            },
            "weighted_exchange_controls": {
                "mode": "Auto (liq+latency)",
                "primary": "Binance",
                "fallback": "Top live venues",
                "note": "Ready for per-exchange manual weighting.",
            },
            "liquidity_event_alerts": {
                "wall_add_remove": "Triggered" if liq_per_min > 6 else "Calm",
                "spread_blowout": "Alert" if spread > 0.2 else "Normal",
                "liq_burst_60s": liq_per_min,
            },
            "signal_confluence_meter": {
                "composite_pct": confluence_score,
                "confidence_pct": confidence_pct,
                "danger_pct": danger_pct,
            },
            "execution_simulator_panel": {
                "estimated_slippage": round(spread * 0.6, 6),
                "fill_probability_pct": fill_probability_pct,
                "mode": "Paper only",
            },
            "workspace_presets": {
                "active_preset": "Default",
                "pair_profile": self.symbol,
                "chart_profile": "Saved state ready",
            },
            "confluence_score": {
                "score": confluence_score,
                "long_bias_pct": round(long_score * 100),
                "short_bias_pct": round(short_score * 100),
            },
            "long_short_checklist": {
                "trend_aligned": regime == "Trend",
                "spread_healthy": spread <= 0.08,
                "liquidity_support": liq_per_min >= 2,
            },
            "entry_quality_meter": {
                "current_quality": entry_quality,
                "spread": spread,
                "trade_speed": trade_rate,
            },
            "regime_detector": {
                "detected_regime": regime,
                "trade_speed": trade_rate,
                "liq_60s": liq_per_min,
            },
            "absorption_vs_breakout": {
                "detector": "Breakout risk" if trade_rate > 10 and spread <= 0.08 else "Absorption bias",
                "absorption_proxy": max(0.0, trade_rate - spread),
                "breakout_pressure_pct": breakout_pressure_pct,
            },
            "spoof_confidence_score": {
                "confidence_pct": spoof_confidence,
                "anomaly_count": self.sync_failures,
                "depth_health": "Stable" if self.book_synced else "Unstable",
            },
            "mtf_alignment": {
                "m1": "Bullish" if last_side == "BUY" else "Bearish",
                "m5": "Aligned" if confidence > 0.55 else "Mixed",
                "m15": "Supportive" if danger_score < 0.4 else "Risky",
            },
            "playbook_tags": {
                "active_playbook": active_playbook,
                "tags": [
                    "trend" if regime == "Trend" else "range",
                    "long-pressure" if last_side == "BUY" else "short-pressure",
                    "tight-spread" if spread <= 0.08 else "wide-spread",
                    "liquidation-active" if liq_per_min > 4 else "calm-flow",
                ],
            },
        }

    def ml_feature_snapshot(self, tab_id: str, source_coverage: dict[str, Any] | None = None) -> dict[str, Any]:
        merged_bids, merged_asks = self._merged_books()
        while self._recent_trade_times and time.monotonic() - self._recent_trade_times[0] > 10:
            self._recent_trade_times.popleft()
        while self._recent_liq_times and time.monotonic() - self._recent_liq_times[0] > 60:
            self._recent_liq_times.popleft()
        trade_rate_10s = len(self._recent_trade_times) / 10.0
        liq_events_60s = len(self._recent_liq_times)

        enabled_exchange_ids: list[str] = []
        connected_exchanges = 0
        if isinstance(source_coverage, dict):
            enabled_exchange_ids = [str(item) for item in source_coverage.get("enabled_exchange_ids", [])]
            connected_exchanges = int(source_coverage.get("connected_exchanges", 0) or 0)

        payload = self._build_ml_snapshot_payload(
            tab_id=tab_id,
            enabled_exchange_ids=enabled_exchange_ids,
            connected_exchanges=connected_exchanges,
            bids=merged_bids,
            asks=merged_asks,
            mark_price=self.mark_price,
            funding_rate=self.funding_rate,
            last_trade_price=self.last_trade_price,
            last_trade_qty=self.last_trade_qty,
            last_trade_side=self.last_trade_side,
            liq_events_60s=float(liq_events_60s),
            last_liquidation_qty=self.last_liquidation_qty,
            last_liquidation_side=self.last_liquidation_side,
        )
        payload["trade_rate_10s"] = float(trade_rate_10s)
        return payload

    def ml_feature_snapshots_by_exchange(
        self,
        tab_id: str,
        source_coverage: dict[str, Any] | None = None,
        exchange_ids: list[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        targets = [str(item).lower() for item in (exchange_ids or []) if str(item).strip()]
        if not targets and isinstance(source_coverage, dict):
            targets = [str(item).lower() for item in source_coverage.get("enabled_exchange_ids", []) if str(item).strip()]
        snapshots: dict[str, dict[str, Any]] = {}
        now = time.monotonic()
        for exchange_id in targets:
            trade_q = self._recent_trade_times_by_exchange.setdefault(exchange_id, deque(maxlen=800))
            liq_q = self._recent_liq_times_by_exchange.setdefault(exchange_id, deque(maxlen=400))
            while trade_q and now - trade_q[0] > 10:
                trade_q.popleft()
            while liq_q and now - liq_q[0] > 60:
                liq_q.popleft()
            trade_rate_10s = len(trade_q) / 10.0
            liq_events_60s = float(len(liq_q))
            bids, asks = self._book_for_exchange(exchange_id)
            mark_tuple = self._external_mark_data.get(exchange_id, (0.0, 0.0, 0.0))
            trade_tuple = self._trade_state_by_exchange.get(exchange_id, (0.0, 0.0, ""))
            liq_tuple = self._liq_state_by_exchange.get(exchange_id, ("", 0.0))
            payload = self._build_ml_snapshot_payload(
                tab_id=tab_id,
                enabled_exchange_ids=[exchange_id],
                connected_exchanges=1 if (bids or asks or trade_rate_10s > 0 or mark_tuple[0] > 0) else 0,
                bids=bids,
                asks=asks,
                mark_price=float(mark_tuple[0] or 0.0),
                funding_rate=float(mark_tuple[1] or 0.0),
                last_trade_price=float(trade_tuple[0] or 0.0),
                last_trade_qty=float(trade_tuple[1] or 0.0),
                last_trade_side=str(trade_tuple[2] or ""),
                liq_events_60s=liq_events_60s,
                last_liquidation_qty=float(liq_tuple[1] or 0.0),
                last_liquidation_side=str(liq_tuple[0] or ""),
            )
            payload["trade_rate_10s"] = float(trade_rate_10s)
            payload["exchange_id"] = exchange_id
            snapshots[exchange_id] = payload
        return snapshots

    def snapshot(self, source_coverage: dict[str, Any] | None = None) -> dict[str, Any]:
        now = time.monotonic()
        while self._recent_trade_times and now - self._recent_trade_times[0] > 10:
            self._recent_trade_times.popleft()
        while self._recent_liq_times and now - self._recent_liq_times[0] > 60:
            self._recent_liq_times.popleft()

        spread = self.best_ask - self.best_bid if self.best_bid and self.best_ask else 0.0
        trade_rate = len(self._recent_trade_times) / 10.0
        liq_per_min = len(self._recent_liq_times)
        merged_bids, merged_asks = self._merged_books()
        enabled_exchange_ids: list[str] = []
        connected_exchanges = 0
        if isinstance(source_coverage, dict):
            enabled_exchange_ids = [str(item) for item in source_coverage.get("enabled_exchange_ids", [])]
            connected_exchanges = int(source_coverage.get("connected_exchanges", 0) or 0)
        ml_payload = self._build_ml_snapshot_payload(
            tab_id="live_terminal",
            enabled_exchange_ids=enabled_exchange_ids,
            connected_exchanges=connected_exchanges,
            bids=merged_bids,
            asks=merged_asks,
            mark_price=self.mark_price,
            funding_rate=self.funding_rate,
            last_trade_price=self.last_trade_price,
            last_trade_qty=self.last_trade_qty,
            last_trade_side=self.last_trade_side,
            liq_events_60s=float(liq_per_min),
            last_liquidation_qty=self.last_liquidation_qty,
            last_liquidation_side=self.last_liquidation_side,
        )

        payload = {
            "type": "terminal_snapshot",
            "symbol": self.symbol,
            "execution_mode": "paper",
            "received_at": utc_now_iso(),
            "event_count": self.event_count,
            "book_synced": self.book_synced,
            "last_update_id": self.last_update_id,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "spread": spread,
            "mark_price": self.mark_price,
            "funding_rate": self.funding_rate,
            "last_trade_price": self.last_trade_price,
            "last_trade_qty": self.last_trade_qty,
            "last_trade_side": self.last_trade_side,
            "last_kline_close": self.last_kline_close,
            "last_kline_interval": self.last_kline_interval,
            "last_liquidation_side": self.last_liquidation_side,
            "last_liquidation_qty": self.last_liquidation_qty,
            "trade_rate_10s": trade_rate,
            "liq_events_60s": liq_per_min,
            "sync_failures": self.sync_failures,
            "stream_counts": dict(self.stream_counts),
            "depth_levels": {
                "bids": len(self.depth_sync.book.bids),
                "asks": len(self.depth_sync.book.asks),
            },
            "decision_layer": self._build_decision_layer(
                spread=spread,
                trade_rate=trade_rate,
                liq_per_min=liq_per_min,
                source_coverage=source_coverage,
            ),
        }
        # Expose ML input features directly on terminal snapshot so live UI cards populate.
        payload.update(
            {
                "return_5s": ml_payload.get("return_5s"),
                "return_15s": ml_payload.get("return_15s"),
                "return_30s": ml_payload.get("return_30s"),
                "return_60s": ml_payload.get("return_60s"),
                "rolling_volatility_30s": ml_payload.get("rolling_volatility_30s"),
                "rolling_volatility_60s": ml_payload.get("rolling_volatility_60s"),
                "candle_range_pct": ml_payload.get("candle_range_pct"),
                "atr_short": ml_payload.get("atr_short"),
                "ema_fast_distance_pct": ml_payload.get("ema_fast_distance_pct"),
                "ema_slow_distance_pct": ml_payload.get("ema_slow_distance_pct"),
                "trend_slope_short": ml_payload.get("trend_slope_short"),
                "vwap_distance_pct": ml_payload.get("vwap_distance_pct"),
                "market_buy_volume_10s": ml_payload.get("market_buy_volume_10s"),
                "market_sell_volume_10s": ml_payload.get("market_sell_volume_10s"),
                "aggressive_buy_sell_delta": ml_payload.get("aggressive_buy_sell_delta"),
                "wall_strength_bid": ml_payload.get("wall_strength_bid"),
                "wall_strength_ask": ml_payload.get("wall_strength_ask"),
                "spread_change_rate": ml_payload.get("spread_change_rate"),
                "cancel_rate_orderbook": ml_payload.get("cancel_rate_orderbook"),
                "session_asia_eu_us": ml_payload.get("session_asia_eu_us"),
                "open_interest_change_pct": ml_payload.get("open_interest_change_pct"),
                "oi_velocity": ml_payload.get("oi_velocity"),
                "funding_rate_change": ml_payload.get("funding_rate_change"),
            }
        )
        return payload

    def maybe_build_heatmap_messages(self) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        now_ms = int(time.time() * 1000)
        if self._last_heatmap_emit_ms and now_ms - self._last_heatmap_emit_ms < self.heatmap_time_bucket_ms:
            return None, None

        merged_bids, merged_asks = self._merged_books()
        if not merged_bids or not merged_asks:
            return None, None

        ref_price = self._reference_price()
        if ref_price <= 0:
            return None, None

        column = self.heatmap_frame_builder.build_column(
            bids=merged_bids,
            asks=merged_asks,
            current_price=ref_price,
            ts_ms=now_ms,
        )
        self.heatmap_store.append_column(column)
        ladder = self.ladder_snapshot_builder.build(
            bids=merged_bids,
            asks=merged_asks,
            current_price=ref_price,
            ts_ms=now_ms,
        )
        ladder["symbol"] = self.symbol
        ladder["bucket_size"] = self.ladder_snapshot_builder.effective_step_size
        self.latest_ladder_snapshot = ladder
        self._last_heatmap_emit_ms = now_ms

        heatmap_frame = {
            "type": "heatmap_frame",
            "symbol": self.symbol,
            "ts_ms": now_ms,
            "bucket_size": self.price_bucketizer.bucket_size,
            "ticks_per_bucket": self.price_bucketizer.ticks_per_bucket,
            "time_bucket_ms": self.heatmap_time_bucket_ms,
            "max_columns": self.heatmap_max_columns,
            "column": column,
        }
        return heatmap_frame, ladder

    def heatmap_bootstrap(self) -> dict[str, Any]:
        ref_price = self._reference_price()
        if self.latest_ladder_snapshot is None and self.book_synced and ref_price > 0:
            self.latest_ladder_snapshot = self.ladder_snapshot_builder.build(
                bids=self.depth_sync.book.bids,
                asks=self.depth_sync.book.asks,
                current_price=ref_price,
                ts_ms=int(time.time() * 1000),
            )
            self.latest_ladder_snapshot["symbol"] = self.symbol
            self.latest_ladder_snapshot["bucket_size"] = self.ladder_snapshot_builder.effective_step_size

        return {
            "type": "heatmap_bootstrap",
            "symbol": self.symbol,
            "bucket_size": self.price_bucketizer.bucket_size,
            "ticks_per_bucket": self.price_bucketizer.ticks_per_bucket,
            "time_bucket_ms": self.heatmap_time_bucket_ms,
            "max_columns": self.heatmap_max_columns,
            "columns": self.heatmap_store.recent_columns(self.heatmap_bootstrap_columns),
            "ladder": self.latest_ladder_snapshot,
            "current_price": ref_price,
        }
