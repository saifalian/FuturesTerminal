from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from app.api.ws_server import BroadcastHub
from app.core.time_utils import utc_now_iso
from app.services.aggregation import SourceCoverageManager
from app.services.aggregation.top20_poller import Top20Poller
from app.services.binance.public_ws import PublicWsClient
from app.services.binance.rest_client import BinanceRestClient
from app.services.market.live_state import LiveTerminalState
from app.services.storage.recorder import RawEventRecorder
from app.services.tool_mode_matrix import event_stream_to_tool_id, is_tool_enabled

logger = logging.getLogger(__name__)

MIN_FEED_DELAY_MS = 100
MAX_FEED_DELAY_MS = 20_000
DEFAULT_ON_SCREEN_DELAY_MS = 500
DEFAULT_BACKGROUND_DELAY_MS = 2_000
DEFAULT_RENDER_DELAY_MS = 500


def clamp_feed_delay_ms(value: int | float | None, default: int) -> int:
    try:
        parsed = int(float(value if value is not None else default))
    except Exception:
        parsed = default
    return max(MIN_FEED_DELAY_MS, min(MAX_FEED_DELAY_MS, parsed))


def normalize_tab_id(raw: str | None) -> str:
    if not raw:
        return "default"
    value = raw.strip().lower()
    if not value:
        return "default"
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", value):
        return "default"
    return value


class SymbolFeedWorker:
    def __init__(
        self,
        symbol: str,
        ws_base: str,
        rest_client: BinanceRestClient,
        recorder: RawEventRecorder,
        cadence_seconds_resolver: Callable[[str], float],
        symbol_active_resolver: Callable[[str], bool],
        tool_mode_matrix_resolver: Callable[[], dict[str, Any]],
    ) -> None:
        self.symbol = symbol.upper()
        self._ws_base = ws_base
        self._rest_client = rest_client
        self._recorder = recorder
        self._cadence_seconds_resolver = cadence_seconds_resolver
        self._symbol_active_resolver = symbol_active_resolver
        self._tool_mode_matrix_resolver = tool_mode_matrix_resolver
        self._subscribers: dict[str, Any] = {}
        self._stop = asyncio.Event()
        self._ws_task: asyncio.Task | None = None
        self._rest_fallback_task: asyncio.Task | None = None
        self._poller = Top20Poller(
            on_orderbook=self._on_orderbook,
            on_trades=self._on_trades,
            on_ohlcv=self._on_ohlcv,
            on_mark_funding=self._on_mark_funding,
            on_liquidations=self._on_liquidations,
            on_stream_mark=self._on_stream_mark,
            on_error=self._on_error,
        )
        self._poller.set_symbol(self.symbol)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    def _subscriber_id(self, session: Any) -> str:
        raw = getattr(session, "session_id", None) or getattr(session, "tab_id", None)
        return str(raw or f"sub-{id(session)}")

    def add_subscriber(self, session: Any) -> None:
        self._subscribers[self._subscriber_id(session)] = session

    def remove_subscriber(self, tab_id: str) -> None:
        self._subscribers.pop(tab_id, None)

    async def start(self) -> None:
        if self._ws_task and not self._ws_task.done():
            return
        self._stop.clear()
        self._poller.set_symbol(self.symbol)
        await self._poller.start()
        client = PublicWsClient(self._ws_base, self.symbol, self._on_binance_event)
        self._ws_task = asyncio.create_task(client.run(self._stop), name=f"symbol-feed-{self.symbol.lower()}")
        self._rest_fallback_task = asyncio.create_task(
            self._run_binance_rest_fallback(), name=f"symbol-feed-rest-fallback-{self.symbol.lower()}"
        )

    async def stop(self) -> None:
        self._stop.set()
        if self._ws_task:
            self._ws_task.cancel()
            await asyncio.gather(self._ws_task, return_exceptions=True)
            self._ws_task = None
        if self._rest_fallback_task:
            self._rest_fallback_task.cancel()
            await asyncio.gather(self._rest_fallback_task, return_exceptions=True)
            self._rest_fallback_task = None
        await self._poller.stop()
        self._subscribers.clear()

    def _subs(self) -> list[Any]:
        return list(self._subscribers.values())

    def _record_external_market_event(
        self,
        *,
        exchange_id: str,
        stream_suffix: str,
        event_type: str,
        payload: dict[str, Any] | list[Any],
        ts_receive_ms: int | None = None,
    ) -> None:
        now_iso = utc_now_iso()
        event: dict[str, Any] = {
            "stream": f"{self.symbol.lower()}@{exchange_id.lower()}@{stream_suffix}",
            "event_type": event_type,
            "payload": payload,
            "received_at": now_iso,
            "source_exchange_id": str(exchange_id).lower(),
            "pair_symbol": self.symbol,
        }
        if ts_receive_ms is not None and ts_receive_ms > 0:
            event["ts_receive_ms"] = int(ts_receive_ms)
        self._recorder.push_nowait(event)

    async def _on_binance_event(self, event: dict[str, Any]) -> None:
        event.setdefault("source_exchange_id", "binance")
        event.setdefault("pair_symbol", self.symbol)
        self._recorder.push_nowait(event)
        for sub in self._subs():
            await sub.handle_binance_event(event, source_symbol=self.symbol)

    async def _run_binance_rest_fallback(self) -> None:
        last_kline_ts_ms = 0
        bootstrapped_klines = False
        last_mark_fetch_mono = 0.0
        last_kline_fetch_mono = 0.0
        while not self._stop.is_set():
            try:
                if self.subscriber_count <= 0 or not self._symbol_active_resolver(self.symbol):
                    await asyncio.sleep(1.0)
                    continue
                now_mono = time.monotonic()
                active_cadence_seconds = self._cadence_seconds_resolver(self.symbol)
                self._poller.set_refresh_cadence_seconds(active_cadence_seconds)

                # Mark/funding/OI are slow-moving; fetch on a calmer cadence.
                if now_mono - last_mark_fetch_mono >= 6.0:
                    premium = await self._rest_client.premium_index(self.symbol)
                    mark_price = float(premium.get("markPrice") or 0.0)
                    funding_rate = float(premium.get("lastFundingRate") or 0.0)
                    oi_payload = await self._rest_client.open_interest(self.symbol)
                    oi_value = float(oi_payload.get("openInterest") or 0.0)
                    # Always forward mark/funding heartbeat so coverage state
                    # does not drop to waiting when upstream formatting varies.
                    self._on_mark_funding("binance", mark_price, funding_rate, oi_value if oi_value > 0 else None)
                    last_mark_fetch_mono = now_mono

                # One-time candle bootstrap to populate chart history.
                if not bootstrapped_klines:
                    seed = await self._rest_client.klines(self.symbol, interval="1m", limit=120)
                    rows: list[dict[str, float | int]] = []
                    for item in seed:
                        if not isinstance(item, (list, tuple)) or len(item) < 5:
                            continue
                        ts_ms = int(item[0] or 0)
                        if ts_ms <= 0:
                            continue
                        rows.append(
                            {
                                "ts_ms": ts_ms,
                                "open": float(item[1] or 0.0),
                                "high": float(item[2] or 0.0),
                                "low": float(item[3] or 0.0),
                                "close": float(item[4] or 0.0),
                            }
                        )
                    if rows:
                        self._on_ohlcv("binance", "1m", rows)
                        last_kline_ts_ms = int(rows[-1]["ts_ms"])
                    bootstrapped_klines = True
                    last_kline_fetch_mono = now_mono
                else:
                    # Adaptive candle cadence from live terminal tab settings.
                    kline_refresh_seconds = active_cadence_seconds
                    if now_mono - last_kline_fetch_mono < kline_refresh_seconds:
                        await asyncio.sleep(0.25)
                        continue
                    klines = await self._rest_client.klines(self.symbol, interval="1m", limit=2)
                    if klines:
                        latest = klines[-1]
                        if isinstance(latest, (list, tuple)) and len(latest) >= 5:
                            ts_ms = int(latest[0] or 0)
                            # Process same-minute updates as well, so candles update smoothly.
                            if ts_ms > 0 and ts_ms >= last_kline_ts_ms:
                                row = {
                                    "ts_ms": ts_ms,
                                    "open": float(latest[1] or 0.0),
                                    "high": float(latest[2] or 0.0),
                                    "low": float(latest[3] or 0.0),
                                    "close": float(latest[4] or 0.0),
                                }
                                self._on_ohlcv("binance", "1m", [row])
                                last_kline_ts_ms = max(last_kline_ts_ms, ts_ms)
                    last_kline_fetch_mono = now_mono
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Keep fallback best-effort; websocket stream remains primary.
                logger.debug("Binance REST fallback error for %s: %s", self.symbol, exc)
            await asyncio.sleep(0.25)

    def _on_orderbook(self, exchange_id: str, bids: list[tuple[float, float]], asks: list[tuple[float, float]]) -> None:
        if not self._mode_allows_stream("book"):
            return
        self._record_external_market_event(
            exchange_id=exchange_id,
            stream_suffix="depth",
            event_type="external_depth",
            payload={"s": self.symbol, "bids": bids, "asks": asks},
        )
        for sub in self._subs():
            sub.handle_external_orderbook(exchange_id, bids, asks, source_symbol=self.symbol)

    def _on_trades(self, exchange_id: str, trades: list[dict]) -> None:
        if not self._mode_allows_stream("trades"):
            return
        ts_receive_ms = int(max((int(item.get("ts_ms", 0) or 0) for item in trades), default=0))
        self._record_external_market_event(
            exchange_id=exchange_id,
            stream_suffix="aggTrade",
            event_type="external_trade_batch",
            payload={"s": self.symbol, "trades": trades},
            ts_receive_ms=ts_receive_ms if ts_receive_ms > 0 else None,
        )
        for sub in self._subs():
            sub.handle_external_trades(exchange_id, trades, source_symbol=self.symbol)

    def _on_ohlcv(self, exchange_id: str, interval: str, rows: list[dict]) -> None:
        if not self._mode_allows_stream("candles"):
            return
        ts_receive_ms = int(max((int(item.get("ts_ms", 0) or 0) for item in rows), default=0))
        self._record_external_market_event(
            exchange_id=exchange_id,
            stream_suffix=f"kline_{interval}",
            event_type="external_ohlcv_batch",
            payload={"s": self.symbol, "interval": interval, "rows": rows},
            ts_receive_ms=ts_receive_ms if ts_receive_ms > 0 else None,
        )
        for sub in self._subs():
            sub.handle_external_ohlcv(exchange_id, interval, rows, source_symbol=self.symbol)

    def _on_mark_funding(
        self,
        exchange_id: str,
        mark_price: float,
        funding_rate: float,
        open_interest: float | None = None,
    ) -> None:
        if not self._mode_allows_stream("mark_funding"):
            return
        self._record_external_market_event(
            exchange_id=exchange_id,
            stream_suffix="markPrice",
            event_type="external_mark_funding",
            payload={
                "s": self.symbol,
                "mark_price": mark_price,
                "funding_rate": funding_rate,
                "open_interest": (float(open_interest) if open_interest is not None else None),
            },
        )
        for sub in self._subs():
            sub.handle_external_mark_funding(
                exchange_id,
                mark_price,
                funding_rate,
                open_interest=open_interest,
                source_symbol=self.symbol,
            )

    def _on_liquidations(self, exchange_id: str, liquidations: list[dict]) -> None:
        if not self._mode_allows_stream("liquidations"):
            return
        ts_receive_ms = int(max((int(item.get("ts_ms", 0) or 0) for item in liquidations), default=0))
        self._record_external_market_event(
            exchange_id=exchange_id,
            stream_suffix="forceOrder",
            event_type="external_liquidation_batch",
            payload={"s": self.symbol, "liquidations": liquidations},
            ts_receive_ms=ts_receive_ms if ts_receive_ms > 0 else None,
        )
        for sub in self._subs():
            sub.handle_external_liquidations(exchange_id, liquidations, source_symbol=self.symbol)

    def _on_stream_mark(self, exchange_id: str, stream: str) -> None:
        tool_id = event_stream_to_tool_id(stream, "")
        if tool_id and (not self._mode_allows_stream(tool_id)):
            return
        for sub in self._subs():
            sub.handle_stream_mark(exchange_id, stream, source_symbol=self.symbol)

    def _on_error(self, exchange_id: str, reason: str) -> None:
        for sub in self._subs():
            sub.handle_poll_error(exchange_id, reason, source_symbol=self.symbol)

    def _mode_allows_stream(self, tool_id: str) -> bool:
        try:
            matrix = self._tool_mode_matrix_resolver()
        except Exception:
            return True
        live_ok = is_tool_enabled(matrix, tool_id=tool_id, mode_id="live")
        bot_ok = is_tool_enabled(matrix, tool_id=tool_id, mode_id="bot")
        recording_ok = is_tool_enabled(matrix, tool_id=tool_id, mode_id="recording")
        return live_ok or bot_ok or recording_ok


@dataclass(slots=True)
class TabSession:
    tab_id: str
    initial_symbol: str
    rest_client: BinanceRestClient
    hub: BroadcastHub
    manager: "TabSessionManager"
    live_state: LiveTerminalState
    source_coverage: SourceCoverageManager
    on_screen_delay_ms: int = DEFAULT_ON_SCREEN_DELAY_MS
    background_delay_ms: int = DEFAULT_BACKGROUND_DELAY_MS
    render_delay_ms: int = DEFAULT_RENDER_DELAY_MS
    is_ws_visible: bool = False
    _current_worker_symbol: str | None = None
    _emit_task: asyncio.Task | None = None
    _next_emit_not_before_ts: float = 0.0
    _emit_min_interval_seconds: float = DEFAULT_RENDER_DELAY_MS / 1000.0
    _closed: bool = False

    @property
    def symbol(self) -> str:
        return self.live_state.symbol

    async def start(self) -> None:
        await self._reconfigure_for_symbol(self.initial_symbol)

    async def close(self) -> None:
        self._closed = True
        if self._emit_task:
            self._emit_task.cancel()
            await asyncio.gather(self._emit_task, return_exceptions=True)
            self._emit_task = None
        if self._current_worker_symbol:
            await self.manager.release_worker(self._current_worker_symbol, self.tab_id)
            self._current_worker_symbol = None

    async def switch_symbol(self, symbol: str) -> tuple[bool, dict[str, Any], dict[str, Any]]:
        target = symbol.upper()
        changed = target != self.symbol
        if changed:
            await self._reconfigure_for_symbol(target)
        resolution = self.source_coverage.last_resolution
        coverage = self.source_coverage.source_coverage_payload()
        await self.hub.broadcast(
            {
                "type": "symbol_changed",
                "symbol": self.symbol,
                "received_at": utc_now_iso(),
            },
            tab_id=self.tab_id,
        )
        await self.hub.broadcast(resolution, tab_id=self.tab_id)
        await self.hub.broadcast(coverage, tab_id=self.tab_id)
        return changed, resolution, coverage

    async def _reconfigure_for_symbol(self, symbol: str) -> None:
        symbol = symbol.upper()
        if self._current_worker_symbol:
            await self.manager.release_worker(self._current_worker_symbol, self.tab_id)
            self._current_worker_symbol = None
        tick_size = self.manager.tick_sizes.get(symbol, 0.1)
        self.live_state.configure_symbol(symbol, tick_size)
        self.source_coverage.resolve_symbol(symbol, self.manager.all_symbols)
        enabled = set(self.source_coverage.enabled_exchange_ids())
        self.live_state.apply_enabled_exchanges(enabled)
        await self.manager.acquire_worker(symbol, self)
        self._current_worker_symbol = symbol
        self.initial_symbol = symbol

    async def apply_exchange_selection(self, exchange_ids: list[str]) -> dict[str, Any]:
        selected = self.source_coverage.set_enabled_exchanges(exchange_ids)
        self.live_state.apply_enabled_exchanges(selected)
        coverage_payload = self.source_coverage.source_coverage_payload()
        await self.hub.broadcast(
            {
                "type": "symbol_changed",
                "symbol": self.symbol,
                "received_at": utc_now_iso(),
            },
            tab_id=self.tab_id,
        )
        await self.hub.broadcast(coverage_payload, tab_id=self.tab_id)
        return {
            "ok": True,
            "enabled_exchange_ids": self.source_coverage.enabled_exchange_ids(),
            "source_coverage": coverage_payload,
        }

    def apply_ladder_range(self, multiplier: int) -> dict[str, Any]:
        result = self.live_state.set_ladder_range_multiplier(multiplier)
        return {
            "ok": True,
            **result,
        }

    def _extract_event_symbol(self, event: dict[str, Any]) -> str | None:
        payload = event.get("payload")
        if not isinstance(payload, dict):
            return None
        top_symbol = payload.get("s")
        if isinstance(top_symbol, str) and top_symbol:
            return top_symbol.upper()
        kline = payload.get("k")
        if isinstance(kline, dict):
            k_symbol = kline.get("s")
            if isinstance(k_symbol, str) and k_symbol:
                return k_symbol.upper()
        force_order = payload.get("o")
        if isinstance(force_order, dict):
            o_symbol = force_order.get("s")
            if isinstance(o_symbol, str) and o_symbol:
                return o_symbol.upper()
        return None

    def _accept_source_symbol(self, source_symbol: str | None, event: dict[str, Any] | None = None) -> bool:
        normalized = source_symbol.upper() if source_symbol else self._extract_event_symbol(event or {})
        target = self.symbol.upper()
        worker_symbol = (self._current_worker_symbol or "").upper()
        if normalized is None:
            return worker_symbol == target
        return normalized == target and (not worker_symbol or normalized == worker_symbol)

    async def handle_binance_event(self, event: dict[str, Any], source_symbol: str | None = None) -> None:
        if self._closed:
            return
        if not self._accept_source_symbol(source_symbol, event):
            return
        if not self.source_coverage.is_exchange_enabled("binance"):
            return
        if not self._mode_allows_stream(event):
            return
        await self.live_state.process_event(event)
        stream = str(event.get("stream", ""))
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        event_type = str(event.get("event_type") or payload.get("e") or "").lower()
        self.source_coverage.mark_event("binance", stream)
        # Some Binance frames can arrive without a reliable `stream` field.
        # Mark coverage by event type as a fallback so coverage does not stay "Waiting".
        if event_type in {"aggtrade", "trade"}:
            self.source_coverage.mark_event("binance", "binance@aggTrade")
        elif event_type == "kline":
            self.source_coverage.mark_event("binance", "binance@kline_1m")
        elif event_type in {"markpriceupdate", "markprice"}:
            self.source_coverage.mark_event("binance", "binance@markPrice")
        elif event_type == "forceorder":
            self.source_coverage.mark_event("binance", "binance@forceOrder")
        if "@aggTrade" not in stream:
            await self.hub.broadcast(
                {
                    "type": "market_event",
                    "symbol": self.symbol,
                    "stream": stream,
                    "event_type": event.get("event_type"),
                    "received_at": event.get("received_at"),
                    "payload": event.get("payload"),
                },
                tab_id=self.tab_id,
            )
        self.request_emit()

    def handle_external_orderbook(
        self,
        exchange_id: str,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
        source_symbol: str | None = None,
    ) -> None:
        if self._closed:
            return
        if not self._accept_source_symbol(source_symbol):
            return
        if not self._mode_allows_stream(f"{exchange_id}@depth"):
            return
        self.live_state.set_external_book(exchange_id, bids, asks)
        self.source_coverage.mark_event(exchange_id, f"{exchange_id}@depth")
        self.source_coverage.mark_event(exchange_id, f"{exchange_id}@bookTicker")
        self.request_emit()

    def handle_external_trades(self, exchange_id: str, trades: list[dict], source_symbol: str | None = None) -> None:
        if self._closed:
            return
        if not self._accept_source_symbol(source_symbol):
            return
        if not self._mode_allows_stream(f"{exchange_id}@aggTrade"):
            return
        self.live_state.ingest_external_trades(exchange_id, trades)
        self.source_coverage.mark_event(exchange_id, f"{exchange_id}@aggTrade")
        self.request_emit()

    def handle_external_ohlcv(
        self,
        exchange_id: str,
        interval: str,
        rows: list[dict],
        source_symbol: str | None = None,
    ) -> None:
        if self._closed:
            return
        if not self._accept_source_symbol(source_symbol):
            return
        if not self._mode_allows_stream(f"{exchange_id}@kline_1m"):
            return
        events = self.live_state.ingest_external_ohlcv(exchange_id, interval, rows)
        # Keep coverage alive when candle data is available, even if there is
        # no brand-new closed candle in this tick.
        if rows:
            self.source_coverage.mark_event(exchange_id, f"{exchange_id}@kline_1m")
        for event in events:
            asyncio.create_task(self.hub.broadcast(event, tab_id=self.tab_id))
        self.request_emit()

    def handle_external_mark_funding(
        self,
        exchange_id: str,
        mark_price: float,
        funding_rate: float,
        open_interest: float | None = None,
        source_symbol: str | None = None,
    ) -> None:
        if self._closed:
            return
        if not self._accept_source_symbol(source_symbol):
            return
        if not self._mode_allows_stream(f"{exchange_id}@markPrice"):
            return
        self.live_state.ingest_external_mark_funding(exchange_id, mark_price, funding_rate, open_interest=open_interest)
        self.source_coverage.mark_event(exchange_id, f"{exchange_id}@markPrice")
        self.request_emit()

    def handle_external_liquidations(
        self,
        exchange_id: str,
        liquidations: list[dict],
        source_symbol: str | None = None,
    ) -> None:
        if self._closed:
            return
        if not self._accept_source_symbol(source_symbol):
            return
        if not self._mode_allows_stream(f"{exchange_id}@forceOrder"):
            return
        self.live_state.ingest_external_liquidations(exchange_id, liquidations)
        self.source_coverage.mark_event(exchange_id, f"{exchange_id}@forceOrder")
        self.request_emit()

    def handle_stream_mark(self, exchange_id: str, stream: str, source_symbol: str | None = None) -> None:
        if self._closed:
            return
        if not self._accept_source_symbol(source_symbol):
            return
        if not self._mode_allows_stream(stream):
            return
        self.source_coverage.mark_event(exchange_id, stream)
        self.request_emit()

    def handle_poll_error(self, exchange_id: str, reason: str, source_symbol: str | None = None) -> None:
        if self._closed:
            return
        if not self._accept_source_symbol(source_symbol):
            return
        self.source_coverage.mark_poll_error(exchange_id, reason)
        self.request_emit()

    def request_emit(self) -> None:
        if self._closed:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        if self._emit_task and not self._emit_task.done():
            return
        now = asyncio.get_running_loop().time()
        delay = max(0.0, self._next_emit_not_before_ts - now)
        self._emit_task = loop.create_task(self._emit_updates(delay), name=f"tab-emit-{self.tab_id}")

    async def _emit_updates(self, delay_seconds: float = 0.0) -> None:
        try:
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
            coverage_payload = self.source_coverage.source_coverage_payload()
            if self.live_state.should_emit_snapshot():
                await self.hub.broadcast(self.live_state.snapshot(source_coverage=coverage_payload), tab_id=self.tab_id)
                await self.hub.broadcast(coverage_payload, tab_id=self.tab_id)
            heatmap_frame, ladder_snapshot = self.live_state.maybe_build_heatmap_messages()
            if heatmap_frame:
                await self.hub.broadcast(heatmap_frame, tab_id=self.tab_id)
            if ladder_snapshot:
                await self.hub.broadcast(ladder_snapshot, tab_id=self.tab_id)
        finally:
            self._next_emit_not_before_ts = asyncio.get_running_loop().time() + self._emit_min_interval_seconds
            self._emit_task = None

    def snapshot(self) -> dict[str, Any]:
        return self.live_state.snapshot(source_coverage=self.source_coverage.source_coverage_payload())

    def heatmap_bootstrap(self) -> dict[str, Any]:
        return self.live_state.heatmap_bootstrap()

    def feed_cadence(self) -> dict[str, int]:
        return {
            "on_screen_ms": clamp_feed_delay_ms(self.on_screen_delay_ms, DEFAULT_ON_SCREEN_DELAY_MS),
            "background_ms": clamp_feed_delay_ms(self.background_delay_ms, DEFAULT_BACKGROUND_DELAY_MS),
            "render_ms": clamp_feed_delay_ms(self.render_delay_ms, DEFAULT_RENDER_DELAY_MS),
        }

    def set_feed_cadence(self, on_screen_ms: int, background_ms: int, render_ms: int | None = None) -> dict[str, int]:
        self.on_screen_delay_ms = clamp_feed_delay_ms(on_screen_ms, DEFAULT_ON_SCREEN_DELAY_MS)
        self.background_delay_ms = clamp_feed_delay_ms(background_ms, DEFAULT_BACKGROUND_DELAY_MS)
        self.render_delay_ms = clamp_feed_delay_ms(render_ms, DEFAULT_RENDER_DELAY_MS)
        self._emit_min_interval_seconds = self.render_delay_ms / 1000.0
        return self.feed_cadence()

    def source_coverage_payload(self) -> dict[str, Any]:
        return self.source_coverage.source_coverage_payload()

    def ml_feature_snapshot(self) -> dict[str, Any]:
        coverage = self.source_coverage.source_coverage_payload()
        return self.live_state.ml_feature_snapshot(tab_id=self.tab_id, source_coverage=coverage)

    def _mode_allows_stream(self, stream: str | dict[str, Any]) -> bool:
        if isinstance(stream, dict):
            stream_name = str(stream.get("stream", ""))
            event_type = str(stream.get("event_type", ""))
        else:
            stream_name = str(stream or "")
            event_type = ""
        tool_id = event_stream_to_tool_id(stream_name, event_type)
        if not tool_id:
            return True
        return is_tool_enabled(self.manager._tool_mode_matrix_resolver(), tool_id=tool_id, mode_id="live")


@dataclass(slots=True)
class BotMarketSession:
    session_id: str
    instance_id: str
    symbol: str
    manager: "TabSessionManager"
    live_state: LiveTerminalState
    source_coverage: SourceCoverageManager
    poll_delay_ms: int = DEFAULT_BACKGROUND_DELAY_MS
    _current_worker_symbol: str | None = None
    _closed: bool = False

    async def configure(self, symbol: str, enabled_exchange_ids: list[str], poll_delay_ms: int | None = None) -> None:
        symbol = symbol.upper()
        if self._current_worker_symbol:
            await self.manager.release_worker(self._current_worker_symbol, self.session_id)
            self._current_worker_symbol = None
        tick_size = self.manager.tick_sizes.get(symbol, 0.1)
        self.live_state.configure_symbol(symbol, tick_size)
        self.source_coverage.resolve_symbol(symbol, self.manager.all_symbols)
        if enabled_exchange_ids:
            enabled = self.source_coverage.set_enabled_exchanges(enabled_exchange_ids)
        else:
            enabled = set(self.source_coverage.enabled_exchange_ids())
        if poll_delay_ms is not None:
            self.poll_delay_ms = clamp_feed_delay_ms(poll_delay_ms, DEFAULT_BACKGROUND_DELAY_MS)
        self.live_state.apply_enabled_exchanges(enabled)
        await self.manager.acquire_worker(symbol, self)
        self._current_worker_symbol = symbol
        self.symbol = symbol
        self._closed = False

    async def close(self) -> None:
        self._closed = True
        if self._current_worker_symbol:
            await self.manager.release_worker(self._current_worker_symbol, self.session_id)
            self._current_worker_symbol = None

    def _accept_source_symbol(self, source_symbol: str | None) -> bool:
        if not source_symbol:
            return False
        return source_symbol.upper() == self.symbol.upper()

    async def handle_binance_event(self, event: dict[str, Any], source_symbol: str | None = None) -> None:
        if self._closed:
            return
        if not self._accept_source_symbol(source_symbol):
            return
        if not self.source_coverage.is_exchange_enabled("binance"):
            return
        if not self._mode_allows_stream(event):
            return
        await self.live_state.process_event(event)
        stream = str(event.get("stream", ""))
        self.source_coverage.mark_event("binance", stream)

    def handle_external_orderbook(
        self,
        exchange_id: str,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
        source_symbol: str | None = None,
    ) -> None:
        if self._closed or not self._accept_source_symbol(source_symbol):
            return
        if not self._mode_allows_stream(f"{exchange_id}@depth"):
            return
        self.live_state.set_external_book(exchange_id, bids, asks)
        self.source_coverage.mark_event(exchange_id, f"{exchange_id}@depth")
        self.source_coverage.mark_event(exchange_id, f"{exchange_id}@bookTicker")

    def handle_external_trades(self, exchange_id: str, trades: list[dict], source_symbol: str | None = None) -> None:
        if self._closed or not self._accept_source_symbol(source_symbol):
            return
        if not self._mode_allows_stream(f"{exchange_id}@aggTrade"):
            return
        self.live_state.ingest_external_trades(exchange_id, trades)
        self.source_coverage.mark_event(exchange_id, f"{exchange_id}@aggTrade")

    def handle_external_ohlcv(
        self,
        exchange_id: str,
        interval: str,
        rows: list[dict],
        source_symbol: str | None = None,
    ) -> None:
        if self._closed or not self._accept_source_symbol(source_symbol):
            return
        if not self._mode_allows_stream(f"{exchange_id}@kline_1m"):
            return
        self.live_state.ingest_external_ohlcv(exchange_id, interval, rows)
        self.source_coverage.mark_event(exchange_id, f"{exchange_id}@kline_1m")

    def handle_external_mark_funding(
        self,
        exchange_id: str,
        mark_price: float,
        funding_rate: float,
        open_interest: float | None = None,
        source_symbol: str | None = None,
    ) -> None:
        if self._closed or not self._accept_source_symbol(source_symbol):
            return
        if not self._mode_allows_stream(f"{exchange_id}@markPrice"):
            return
        self.live_state.ingest_external_mark_funding(exchange_id, mark_price, funding_rate, open_interest=open_interest)
        self.source_coverage.mark_event(exchange_id, f"{exchange_id}@markPrice")

    def handle_external_liquidations(
        self,
        exchange_id: str,
        liquidations: list[dict],
        source_symbol: str | None = None,
    ) -> None:
        if self._closed or not self._accept_source_symbol(source_symbol):
            return
        if not self._mode_allows_stream(f"{exchange_id}@forceOrder"):
            return
        self.live_state.ingest_external_liquidations(exchange_id, liquidations)
        self.source_coverage.mark_event(exchange_id, f"{exchange_id}@forceOrder")

    def handle_stream_mark(self, exchange_id: str, stream: str, source_symbol: str | None = None) -> None:
        if self._closed or not self._accept_source_symbol(source_symbol):
            return
        if not self._mode_allows_stream(stream):
            return
        self.source_coverage.mark_event(exchange_id, stream)

    def handle_poll_error(self, exchange_id: str, reason: str, source_symbol: str | None = None) -> None:
        if self._closed or not self._accept_source_symbol(source_symbol):
            return
        self.source_coverage.mark_poll_error(exchange_id, reason)

    def ml_feature_snapshot(self) -> dict[str, Any]:
        coverage = self.source_coverage.source_coverage_payload()
        return self.live_state.ml_feature_snapshot(tab_id=self.session_id, source_coverage=coverage)

    def ml_feature_snapshots_by_exchange(self, exchange_ids: list[str] | None = None) -> dict[str, dict[str, Any]]:
        coverage = self.source_coverage.source_coverage_payload()
        return self.live_state.ml_feature_snapshots_by_exchange(
            tab_id=self.session_id,
            source_coverage=coverage,
            exchange_ids=exchange_ids,
        )

    def _mode_allows_stream(self, stream: str | dict[str, Any]) -> bool:
        if isinstance(stream, dict):
            stream_name = str(stream.get("stream", ""))
            event_type = str(stream.get("event_type", ""))
        else:
            stream_name = str(stream or "")
            event_type = ""
        tool_id = event_stream_to_tool_id(stream_name, event_type)
        if not tool_id:
            return True
        return is_tool_enabled(self.manager._tool_mode_matrix_resolver(), tool_id=tool_id, mode_id="bot")


class TabSessionManager:
    def __init__(
        self,
        ws_base: str,
        rest_client: BinanceRestClient,
        hub: BroadcastHub,
        recorder: RawEventRecorder,
        all_symbols: list[str],
        tick_sizes: dict[str, float],
        default_symbol: str,
        selection_path: Path | None = None,
        tool_mode_matrix_resolver: Callable[[], dict[str, Any]] | None = None,
    ) -> None:
        self._ws_base = ws_base
        self._rest_client = rest_client
        self._hub = hub
        self._recorder = recorder
        self.all_symbols = [symbol.upper() for symbol in all_symbols]
        self.tick_sizes = {symbol.upper(): value for symbol, value in tick_sizes.items()}
        self.default_symbol = default_symbol.upper()
        self._selection_path = selection_path
        self._tool_mode_matrix_resolver = tool_mode_matrix_resolver or (lambda: {})
        self._sessions: dict[str, TabSession] = {}
        self._bot_sessions: dict[str, BotMarketSession] = {}
        self._workers: dict[str, SymbolFeedWorker] = {}
        self._lock = asyncio.Lock()

    def update_market_metadata(self, all_symbols: list[str], tick_sizes: dict[str, float]) -> None:
        self.all_symbols = [symbol.upper() for symbol in all_symbols]
        self.tick_sizes = {symbol.upper(): value for symbol, value in tick_sizes.items()}

    def _resolve_effective_symbol_cadence_seconds(self, symbol: str) -> float:
        symbol_up = symbol.upper()
        on_screen_values: list[int] = []
        background_values: list[int] = []
        for session in self._sessions.values():
            if session.symbol.upper() != symbol_up:
                continue
            cadence = session.feed_cadence()
            if session.is_ws_visible:
                on_screen_values.append(int(cadence["on_screen_ms"]))
            else:
                background_values.append(int(cadence["background_ms"]))
        for session in self._bot_sessions.values():
            if session.symbol.upper() != symbol_up:
                continue
            background_values.append(int(clamp_feed_delay_ms(session.poll_delay_ms, DEFAULT_BACKGROUND_DELAY_MS)))
        if on_screen_values:
            return max(MIN_FEED_DELAY_MS, min(on_screen_values)) / 1000.0
        if background_values:
            return max(MIN_FEED_DELAY_MS, min(background_values)) / 1000.0
        return DEFAULT_BACKGROUND_DELAY_MS / 1000.0

    def _is_symbol_active_in_sessions(self, symbol: str) -> bool:
        symbol_up = symbol.upper()
        for session in self._sessions.values():
            if session.symbol.upper() == symbol_up and session.is_ws_visible:
                return True
        for session in self._bot_sessions.values():
            if session.symbol.upper() == symbol_up:
                return True
        return False

    async def get_or_create(self, tab_id: str | None, initial_symbol: str | None = None) -> TabSession:
        normalized = normalize_tab_id(tab_id)
        requested_symbol = str(initial_symbol or "").upper().strip()
        effective_symbol = requested_symbol if requested_symbol in self.all_symbols else self.default_symbol
        async with self._lock:
            existing = self._sessions.get(normalized)
            if existing:
                return existing
            session = TabSession(
                tab_id=normalized,
                initial_symbol=effective_symbol,
                rest_client=self._rest_client,
                hub=self._hub,
                manager=self,
                live_state=LiveTerminalState(symbol=effective_symbol, rest_client=self._rest_client),
                source_coverage=SourceCoverageManager(selection_path=self._selection_path),
            )
            self._sessions[normalized] = session
        await session.start()
        return session

    async def close_tab(self, tab_id: str | None) -> bool:
        normalized = normalize_tab_id(tab_id)
        if normalized == "default":
            return False
        async with self._lock:
            session = self._sessions.pop(normalized, None)
        if not session:
            return False
        await session.close()
        return True

    async def acquire_worker(self, symbol: str, session: TabSession) -> None:
        symbol = symbol.upper()
        created = False
        async with self._lock:
            worker = self._workers.get(symbol)
            if worker is None:
                worker = SymbolFeedWorker(
                    symbol=symbol,
                    ws_base=self._ws_base,
                    rest_client=self._rest_client,
                    recorder=self._recorder,
                    cadence_seconds_resolver=self._resolve_effective_symbol_cadence_seconds,
                    symbol_active_resolver=self._is_symbol_active_in_sessions,
                    tool_mode_matrix_resolver=self._tool_mode_matrix_resolver,
                )
                self._workers[symbol] = worker
                created = True
            worker.add_subscriber(session)
        if created:
            await worker.start()

    async def release_worker(self, symbol: str, tab_id: str) -> None:
        symbol = symbol.upper()
        worker_to_stop: SymbolFeedWorker | None = None
        async with self._lock:
            worker = self._workers.get(symbol)
            if not worker:
                return
            worker.remove_subscriber(tab_id)
            if worker.subscriber_count == 0:
                worker_to_stop = worker
                self._workers.pop(symbol, None)
        if worker_to_stop is not None:
            await worker_to_stop.stop()

    async def switch_symbol(self, tab_id: str | None, symbol: str) -> dict[str, Any]:
        session = await self.get_or_create(tab_id)
        changed, resolution, source_coverage = await session.switch_symbol(symbol)
        return {
            "ok": True,
            "changed": changed,
            "active_symbol": session.symbol,
            "symbol_resolution": resolution,
            "source_coverage": source_coverage,
        }

    async def apply_exchange_selection(self, tab_id: str | None, exchange_ids: list[str]) -> dict[str, Any]:
        session = await self.get_or_create(tab_id)
        return await session.apply_exchange_selection(exchange_ids)

    async def apply_ladder_range(self, tab_id: str | None, multiplier: int) -> dict[str, Any]:
        session = await self.get_or_create(tab_id)
        return session.apply_ladder_range(multiplier)

    async def snapshot(self, tab_id: str | None) -> dict[str, Any]:
        session = await self.get_or_create(tab_id)
        return session.snapshot()

    async def heatmap_bootstrap(self, tab_id: str | None) -> dict[str, Any]:
        session = await self.get_or_create(tab_id)
        return session.heatmap_bootstrap()

    async def source_coverage(self, tab_id: str | None) -> dict[str, Any]:
        session = await self.get_or_create(tab_id)
        return session.source_coverage_payload()

    async def get_feed_cadence(self, tab_id: str | None) -> dict[str, int]:
        session = await self.get_or_create(tab_id)
        return session.feed_cadence()

    async def set_feed_cadence(
        self, tab_id: str | None, on_screen_ms: int, background_ms: int, render_ms: int | None = None
    ) -> dict[str, int]:
        session = await self.get_or_create(tab_id)
        cadence = session.set_feed_cadence(on_screen_ms, background_ms, render_ms=render_ms)
        return cadence

    async def set_tab_visibility(self, tab_id: str | None, visible: bool) -> None:
        normalized = normalize_tab_id(tab_id)
        session = self._sessions.get(normalized)
        if session is None:
            if not visible:
                return
            session = await self.get_or_create(normalized)
        session.is_ws_visible = bool(visible)

    async def last_resolution(self, tab_id: str | None) -> dict[str, Any]:
        session = await self.get_or_create(tab_id)
        return session.source_coverage.last_resolution

    async def active_symbol(self, tab_id: str | None) -> str:
        session = await self.get_or_create(tab_id)
        return session.symbol

    async def shutdown(self) -> None:
        async with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
            bot_sessions = list(self._bot_sessions.values())
            self._bot_sessions.clear()
            workers = list(self._workers.values())
            self._workers.clear()
        for session in sessions:
            await session.close()
        for session in bot_sessions:
            await session.close()
        for worker in workers:
            await worker.stop()

    async def list_pairs(self) -> list[str]:
        async with self._lock:
            symbols = sorted({session.symbol for session in self._sessions.values() if session.symbol})
        return symbols

    async def collect_ml_snapshots(self) -> list[dict[str, Any]]:
        state = self._recorder.recording_state()
        recording_enabled = bool(state.get("enabled", False))
        recording_symbol = str(state.get("symbol", "")).upper()
        if not recording_enabled:
            return []
        async with self._lock:
            sessions = list(self._sessions.values())
        snapshots: list[dict[str, Any]] = []
        for session in sessions:
            if recording_symbol and session.symbol.upper() != recording_symbol:
                continue
            try:
                snapshots.append(session.ml_feature_snapshot())
            except Exception:
                continue
        return snapshots

    async def latest_pair_snapshot(self, pair_symbol: str) -> dict[str, Any] | None:
        target = pair_symbol.upper()
        async with self._lock:
            sessions = [session for session in self._sessions.values() if session.symbol == target]
        if not sessions:
            return None
        try:
            return sessions[0].ml_feature_snapshot()
        except Exception:
            return None

    async def acquire_bot_session(
        self,
        instance_id: str,
        pair_symbol: str,
        enabled_exchange_ids: list[str],
        *,
        poll_delay_ms: int | None = None,
    ) -> None:
        symbol = pair_symbol.upper()
        key = f"ml-bot-{instance_id}"
        async with self._lock:
            session = self._bot_sessions.get(instance_id)
            if session is None:
                session = BotMarketSession(
                    session_id=key,
                    instance_id=instance_id,
                    symbol=symbol,
                    manager=self,
                    live_state=LiveTerminalState(symbol=symbol, rest_client=self._rest_client),
                    source_coverage=SourceCoverageManager(selection_path=self._selection_path),
                )
                self._bot_sessions[instance_id] = session
        await session.configure(symbol, enabled_exchange_ids, poll_delay_ms=poll_delay_ms)

    async def release_bot_session(self, instance_id: str) -> None:
        async with self._lock:
            session = self._bot_sessions.pop(instance_id, None)
        if session is not None:
            await session.close()

    async def bot_pair_snapshot(self, instance_id: str) -> dict[str, Any] | None:
        async with self._lock:
            session = self._bot_sessions.get(instance_id)
        if session is None:
            return None
        try:
            return session.ml_feature_snapshot()
        except Exception:
            return None

    async def bot_pair_snapshots_by_exchange(
        self,
        instance_id: str,
        exchange_ids: list[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        async with self._lock:
            session = self._bot_sessions.get(instance_id)
        if session is None:
            return {}
        try:
            return session.ml_feature_snapshots_by_exchange(exchange_ids=exchange_ids)
        except Exception:
            return {}
