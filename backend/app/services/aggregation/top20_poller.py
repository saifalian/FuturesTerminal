from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Callable


logger = logging.getLogger(__name__)

try:
    import ccxt  # type: ignore
except Exception:  # pragma: no cover - optional dependency in dev
    ccxt = None  # type: ignore


@dataclass(slots=True)
class ExchangeSpec:
    exchange_id: str
    ccxt_id: str


TOP20_SPECS: tuple[ExchangeSpec, ...] = (
    ExchangeSpec("bitmart", "bitmart"),
    ExchangeSpec("bybit", "bybit"),
    ExchangeSpec("coinw", "coinw"),
    ExchangeSpec("mexc", "mexc"),
    ExchangeSpec("gate", "gateio"),
    ExchangeSpec("bydfi", "bydfi"),
    ExchangeSpec("hyperliquid", "hyperliquid"),
    ExchangeSpec("weex", "weex"),
    ExchangeSpec("lbank", "lbank"),
    ExchangeSpec("okx", "okx"),
    ExchangeSpec("bitget", "bitget"),
    ExchangeSpec("kucoin", "kucoinfutures"),
    ExchangeSpec("xt", "xt"),
    ExchangeSpec("htx", "htx"),
    ExchangeSpec("whitebit", "whitebit"),
    ExchangeSpec("coinup", "coinup"),
    ExchangeSpec("orangex", "orangex"),
    ExchangeSpec("bingx", "bingx"),
    ExchangeSpec("toobit", "toobit"),
)


class Top20Poller:
    def __init__(
        self,
        on_orderbook: Callable[[str, list[tuple[float, float]], list[tuple[float, float]]], None],
        on_trades: Callable[[str, list[dict]], None],
        on_ohlcv: Callable[[str, str, list[dict]], None],
        on_mark_funding: Callable[[str, float, float], None],
        on_liquidations: Callable[[str, list[dict]], None],
        on_stream_mark: Callable[[str, str], None],
        on_error: Callable[[str, str], None],
    ) -> None:
        self._on_orderbook = on_orderbook
        self._on_trades = on_trades
        self._on_ohlcv = on_ohlcv
        self._on_mark_funding = on_mark_funding
        self._on_liquidations = on_liquidations
        self._on_stream_mark = on_stream_mark
        self._on_error = on_error
        self._enabled = ccxt is not None
        self._symbol = "BTCUSDT"
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._specs: list[ExchangeSpec] = [spec for spec in TOP20_SPECS]
        self._enabled_exchange_ids: set[str] = {spec.exchange_id for spec in self._specs}
        self._exchange_clients: dict[str, object] = {}
        self._resolved_symbols: dict[str, str | None] = {}
        self._clients_ready = False
        self._concurrency = 6
        self._last_feature_poll_ts: dict[str, dict[str, float]] = {}
        self._base_intervals_sec = {
            "orderbook": 2.2,
            "trades": 2.5,
            "ohlcv": 4.0,
            "mark": 6.0,
            "liquidations": 4.0,
        }
        self._intervals_sec = dict(self._base_intervals_sec)
        self._loop_sleep_sec = 0.8

    @property
    def enabled(self) -> bool:
        return self._enabled

    def set_symbol(self, symbol: str) -> None:
        self._symbol = symbol.upper()
        self._resolved_symbols.clear()

    def set_enabled_exchanges(self, exchange_ids: set[str]) -> None:
        self._enabled_exchange_ids = {spec.exchange_id for spec in self._specs if spec.exchange_id in exchange_ids}
        disabled = {spec.exchange_id for spec in self._specs if spec.exchange_id not in self._enabled_exchange_ids}
        for exchange_id in disabled:
            self._last_feature_poll_ts.pop(exchange_id, None)

    def set_refresh_cadence_seconds(self, seconds: float) -> None:
        # Clamp requested cadence to avoid pathological poll loops.
        safe = max(0.1, min(20.0, float(seconds)))
        scale = safe / 0.5
        self._loop_sleep_sec = max(0.1, 0.8 * scale)
        self._intervals_sec = {
            name: max(0.2, base * scale)
            for name, base in self._base_intervals_sec.items()
        }

    async def start(self) -> None:
        if not self._enabled:
            logger.warning("ccxt is not installed; top20 poller disabled")
            return
        if self._task and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="top20-poller")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        for exchange in self._exchange_clients.values():
            try:
                await asyncio.to_thread(exchange.close)
            except Exception:
                pass
        self._exchange_clients.clear()
        self._resolved_symbols.clear()
        self._clients_ready = False

    def _target_base(self) -> str:
        return self._symbol[:-4] if self._symbol.endswith("USDT") else self._symbol

    def _select_symbol(self, markets: dict, base: str) -> str | None:
        direct_candidates = (
            f"{base}/USDT:USDT",
            f"{base}/USDT",
            f"{base}/USDT-PERP",
            f"{base}/USDTM",
            f"{base}USDT",
        )
        for candidate in direct_candidates:
            if candidate in markets:
                return candidate
        for symbol, market in markets.items():
            try:
                market_base = str(market.get("base", "")).upper()
                market_quote = str(market.get("quote", "")).upper()
                market_settle = str(market.get("settle", "")).upper()
                is_contract = bool(market.get("contract", False)) or bool(market.get("swap", False)) or bool(market.get("future", False))
                if market_base == base and market_quote == "USDT" and (market_settle in {"", "USDT"}) and is_contract:
                    return symbol
            except Exception:
                continue
        return None

    async def _run(self) -> None:
        if not self._clients_ready:
            await self._init_clients()
        semaphore = asyncio.Semaphore(self._concurrency)
        while not self._stop.is_set():
            tasks = [
                asyncio.create_task(self._poll_exchange_with_limit(spec, semaphore))
                for spec in self._specs
                if spec.exchange_id in self._enabled_exchange_ids
            ]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(self._loop_sleep_sec)

    def _feature_due(self, exchange_id: str, feature: str) -> bool:
        now = time.monotonic()
        feature_state = self._last_feature_poll_ts.setdefault(exchange_id, {})
        last_ts = feature_state.get(feature, 0.0)
        interval = self._intervals_sec.get(feature, 2.0)
        if (now - last_ts) >= interval:
            feature_state[feature] = now
            return True
        return False

    async def _init_clients(self) -> None:
        if ccxt is None:
            return
        for spec in self._specs:
            exchange_ctor = getattr(ccxt, spec.ccxt_id, None)
            if exchange_ctor is None:
                self._on_error(spec.exchange_id, "adapter_unavailable")
                continue
            try:
                options: dict[str, object] = {"enableRateLimit": True, "timeout": 8000}
                if spec.ccxt_id in {"bybit", "bitget", "mexc", "okx", "gateio", "kucoinfutures", "bingx", "toobit"}:
                    options["options"] = {"defaultType": "swap"}
                exchange = exchange_ctor(options)
                self._exchange_clients[spec.exchange_id] = exchange
            except Exception:
                self._on_error(spec.exchange_id, "client_init_failed")
        self._clients_ready = True

    async def _poll_exchange_with_limit(self, spec: ExchangeSpec, semaphore: asyncio.Semaphore) -> None:
        async with semaphore:
            await self._poll_exchange(spec)

    async def _poll_exchange(self, spec: ExchangeSpec) -> None:
        if ccxt is None or self._stop.is_set():
            return
        exchange = self._exchange_clients.get(spec.exchange_id)
        if exchange is None:
            return
        try:
            base = self._target_base()
            if not getattr(exchange, "markets", None):
                await asyncio.to_thread(exchange.load_markets)
            cached_symbol = self._resolved_symbols.get(spec.exchange_id)
            symbol = cached_symbol if cached_symbol else self._select_symbol(exchange.markets, base)
            if not symbol:
                self._resolved_symbols[spec.exchange_id] = None
                self._on_error(spec.exchange_id, "pair_not_supported")
                return
            self._resolved_symbols[spec.exchange_id] = symbol
            limit = 60
            if spec.ccxt_id == "kucoinfutures":
                limit = 20
            elif spec.ccxt_id == "bitget":
                limit = 100
            if self._feature_due(spec.exchange_id, "orderbook"):
                orderbook = await asyncio.to_thread(exchange.fetch_order_book, symbol, limit)
                bids = [(float(price), float(qty)) for price, qty in orderbook.get("bids", [])[:40] if qty and price]
                asks = [(float(price), float(qty)) for price, qty in orderbook.get("asks", [])[:40] if qty and price]
                if not bids or not asks:
                    self._on_error(spec.exchange_id, "no_depth_data")
                    return
                self._on_orderbook(spec.exchange_id, bids, asks)
                self._on_stream_mark(spec.exchange_id, f"{spec.exchange_id}@depth")
                self._on_stream_mark(spec.exchange_id, f"{spec.exchange_id}@bookTicker")

            if self._feature_due(spec.exchange_id, "trades"):
                trades = await self._fetch_trades(exchange, symbol)
                if trades:
                    self._on_trades(spec.exchange_id, trades)
                    self._on_stream_mark(spec.exchange_id, f"{spec.exchange_id}@aggTrade")

            if self._feature_due(spec.exchange_id, "ohlcv"):
                ohlcv_rows = await self._fetch_ohlcv(exchange, symbol)
                if ohlcv_rows:
                    self._on_ohlcv(spec.exchange_id, "1m", ohlcv_rows)
                    self._on_stream_mark(spec.exchange_id, f"{spec.exchange_id}@kline_1m")

            if self._feature_due(spec.exchange_id, "mark"):
                mark_price, funding_rate = await self._fetch_mark_funding(exchange, symbol)
                if mark_price > 0 or funding_rate != 0.0:
                    self._on_mark_funding(spec.exchange_id, mark_price, funding_rate)
                    self._on_stream_mark(spec.exchange_id, f"{spec.exchange_id}@markPrice")

            if self._feature_due(spec.exchange_id, "liquidations"):
                liqs = await self._fetch_liquidations(exchange, symbol)
                if liqs:
                    self._on_liquidations(spec.exchange_id, liqs)
                    self._on_stream_mark(spec.exchange_id, f"{spec.exchange_id}@forceOrder")
        except Exception as exc:
            logger.debug("Top20 poll error for %s: %s", spec.exchange_id, exc)
            self._on_error(spec.exchange_id, "poll_error")

    async def _fetch_trades(self, exchange: object, symbol: str) -> list[dict]:
        has_map = getattr(exchange, "has", {}) or {}
        if not bool(has_map.get("fetchTrades")):
            return []
        try:
            raw = await asyncio.to_thread(exchange.fetch_trades, symbol, None, 80)
        except Exception:
            return []
        out: list[dict] = []
        for item in raw or []:
            try:
                ts_ms = int(item.get("timestamp") or 0)
                if ts_ms <= 0:
                    continue
                price = float(item.get("price") or 0)
                qty = float(item.get("amount") or 0)
                if price <= 0 or qty <= 0:
                    continue
                side = str(item.get("side") or "").upper()
                if side not in {"BUY", "SELL"}:
                    side = ""
                out.append({"ts_ms": ts_ms, "price": price, "qty": qty, "side": side})
            except Exception:
                continue
        return out

    async def _fetch_ohlcv(self, exchange: object, symbol: str) -> list[dict]:
        has_map = getattr(exchange, "has", {}) or {}
        if not bool(has_map.get("fetchOHLCV")):
            return []
        try:
            raw = await asyncio.to_thread(exchange.fetch_ohlcv, symbol, "1m", None, 6)
        except Exception:
            return []
        out: list[dict] = []
        for row in raw or []:
            try:
                if not isinstance(row, (list, tuple)) or len(row) < 5:
                    continue
                ts_ms = int(row[0] or 0)
                o = float(row[1] or 0)
                h = float(row[2] or 0)
                l = float(row[3] or 0)
                c = float(row[4] or 0)
                if ts_ms <= 0:
                    continue
                out.append({"ts_ms": ts_ms, "open": o, "high": h, "low": l, "close": c})
            except Exception:
                continue
        return out

    async def _fetch_mark_funding(self, exchange: object, symbol: str) -> tuple[float, float]:
        has_map = getattr(exchange, "has", {}) or {}
        if not bool(has_map.get("fetchFundingRate")):
            return 0.0, 0.0
        try:
            raw = await asyncio.to_thread(exchange.fetch_funding_rate, symbol)
        except Exception:
            return 0.0, 0.0

        mark_price = 0.0
        funding_rate = 0.0
        try:
            mark_price = float(raw.get("markPrice") or raw.get("indexPrice") or 0.0)
        except Exception:
            mark_price = 0.0
        try:
            funding_rate = float(raw.get("fundingRate") or 0.0)
        except Exception:
            funding_rate = 0.0
        return mark_price, funding_rate

    async def _fetch_liquidations(self, exchange: object, symbol: str) -> list[dict]:
        has_map = getattr(exchange, "has", {}) or {}
        if not bool(has_map.get("fetchLiquidations")):
            return []
        try:
            raw = await asyncio.to_thread(exchange.fetch_liquidations, symbol, None, 40)
        except Exception:
            return []
        out: list[dict] = []
        for item in raw or []:
            try:
                ts_ms = int(item.get("timestamp") or 0)
                if ts_ms <= 0:
                    continue
                qty = float(item.get("contracts") or item.get("amount") or 0.0)
                side = str(item.get("side") or "").upper()
                if side not in {"BUY", "SELL"}:
                    side = ""
                out.append({"ts_ms": ts_ms, "qty": qty, "side": side})
            except Exception:
                continue
        return out
