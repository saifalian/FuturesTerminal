from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from app.services.aggregation.manager import TOP20_EXCHANGES

logger = logging.getLogger(__name__)

try:
    import ccxt  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    ccxt = None  # type: ignore


CCXT_EXCHANGE_MAP: dict[str, str] = {
    "binance": "binanceusdm",
    "bitmart": "bitmart",
    "bybit": "bybit",
    "coinw": "coinw",
    "mexc": "mexc",
    "gate": "gateio",
    "bydfi": "bydfi",
    "hyperliquid": "hyperliquid",
    "weex": "weex",
    "lbank": "lbank",
    "okx": "okx",
    "bitget": "bitget",
    "kucoin": "kucoinfutures",
    "xt": "xt",
    "htx": "htx",
    "whitebit": "whitebit",
    "coinup": "coinup",
    "orangex": "orangex",
    "bingx": "bingx",
    "toobit": "toobit",
}


ControlHook = Callable[[], Awaitable[str]]
LogHook = Callable[[str], Awaitable[None]]


@dataclass(slots=True)
class ExchangeFetchOutput:
    exchange_id: str
    exchange_name: str
    status: str
    reason: str
    rows: list[dict[str, Any]]
    data_hours: float
    features_available: dict[str, bool]
    resolved_symbol: str | None
    started_at_ms: int
    ended_at_ms: int


class MlHistoricFetcher:
    def __init__(self) -> None:
        self._exchange_names = {exchange_id: name for exchange_id, name, _ in TOP20_EXCHANGES}
        self._max_ohlcv_pages = 3000
        self._max_trades_pages = 150
        self._max_funding_pages = 120
        self._max_liq_pages = 120

    async def fetch_exchange_hybrid(
        self,
        *,
        exchange_id: str,
        pair_symbol: str,
        since_ms: int,
        until_ms: int,
        control_hook: ControlHook | None = None,
        log_hook: LogHook | None = None,
    ) -> ExchangeFetchOutput:
        exchange_key = exchange_id.lower()
        exchange_name = self._exchange_names.get(exchange_key, exchange_key.upper())
        started_at = int(time.time() * 1000)
        if ccxt is None:
            return ExchangeFetchOutput(
                exchange_id=exchange_key,
                exchange_name=exchange_name,
                status="failed",
                reason="ccxt_not_available",
                rows=[],
                data_hours=0.0,
                features_available={"ohlcv": False, "trades": False, "funding": False, "liquidations": False},
                resolved_symbol=None,
                started_at_ms=started_at,
                ended_at_ms=int(time.time() * 1000),
            )

        ccxt_id = CCXT_EXCHANGE_MAP.get(exchange_key)
        if not ccxt_id:
            return ExchangeFetchOutput(
                exchange_id=exchange_key,
                exchange_name=exchange_name,
                status="failed",
                reason="exchange_mapping_missing",
                rows=[],
                data_hours=0.0,
                features_available={"ohlcv": False, "trades": False, "funding": False, "liquidations": False},
                resolved_symbol=None,
                started_at_ms=started_at,
                ended_at_ms=int(time.time() * 1000),
            )

        exchange_ctor = getattr(ccxt, ccxt_id, None)
        if exchange_ctor is None:
            return ExchangeFetchOutput(
                exchange_id=exchange_key,
                exchange_name=exchange_name,
                status="failed",
                reason="adapter_unavailable",
                rows=[],
                data_hours=0.0,
                features_available={"ohlcv": False, "trades": False, "funding": False, "liquidations": False},
                resolved_symbol=None,
                started_at_ms=started_at,
                ended_at_ms=int(time.time() * 1000),
            )

        exchange = None
        try:
            options: dict[str, Any] = {"enableRateLimit": True, "timeout": 12000}
            if ccxt_id in {"bybit", "bitget", "mexc", "okx", "gateio", "kucoinfutures", "bingx", "toobit", "binanceusdm"}:
                options["options"] = {"defaultType": "swap"}
            exchange = exchange_ctor(options)
            await asyncio.to_thread(exchange.load_markets)
            await self._wait_control(control_hook)
            symbol = self._select_symbol(exchange.markets, pair_symbol)
            if not symbol:
                return ExchangeFetchOutput(
                    exchange_id=exchange_key,
                    exchange_name=exchange_name,
                    status="failed",
                    reason="pair_not_supported",
                    rows=[],
                    data_hours=0.0,
                    features_available={"ohlcv": False, "trades": False, "funding": False, "liquidations": False},
                    resolved_symbol=None,
                    started_at_ms=started_at,
                    ended_at_ms=int(time.time() * 1000),
                )
            if log_hook:
                await log_hook(f"{exchange_key}: resolved symbol {symbol}")
            features = getattr(exchange, "has", {}) or {}

            candles = await self._fetch_ohlcv_history(
                exchange=exchange,
                symbol=symbol,
                since_ms=since_ms,
                until_ms=until_ms,
                control_hook=control_hook,
                log_hook=log_hook,
                exchange_id=exchange_key,
                enabled=bool(features.get("fetchOHLCV")),
            )
            if not candles:
                return ExchangeFetchOutput(
                    exchange_id=exchange_key,
                    exchange_name=exchange_name,
                    status="failed",
                    reason="no_ohlcv_data",
                    rows=[],
                    data_hours=0.0,
                    features_available={"ohlcv": False, "trades": False, "funding": False, "liquidations": False},
                    resolved_symbol=symbol,
                    started_at_ms=started_at,
                    ended_at_ms=int(time.time() * 1000),
                )

            trades_by_minute: dict[int, tuple[int, float]] = {}
            if bool(features.get("fetchTrades")):
                trades_by_minute = await self._fetch_trades_per_minute(
                    exchange=exchange,
                    symbol=symbol,
                    since_ms=since_ms,
                    until_ms=until_ms,
                    control_hook=control_hook,
                    log_hook=log_hook,
                    exchange_id=exchange_key,
                )
            funding_by_minute: dict[int, float] = {}
            has_funding_history = bool(features.get("fetchFundingRateHistory"))
            has_funding_single = bool(features.get("fetchFundingRate"))
            if has_funding_history:
                funding_by_minute = await self._fetch_funding_per_minute(
                    exchange=exchange,
                    symbol=symbol,
                    since_ms=since_ms,
                    until_ms=until_ms,
                    control_hook=control_hook,
                    log_hook=log_hook,
                    exchange_id=exchange_key,
                )
            elif has_funding_single:
                funding_by_minute = await self._fetch_current_funding(exchange=exchange, symbol=symbol)

            liq_by_minute: dict[int, int] = {}
            if bool(features.get("fetchLiquidations")):
                liq_by_minute = await self._fetch_liquidations_per_minute(
                    exchange=exchange,
                    symbol=symbol,
                    since_ms=since_ms,
                    until_ms=until_ms,
                    control_hook=control_hook,
                    log_hook=log_hook,
                    exchange_id=exchange_key,
                )

            rows = self._build_feature_rows(
                pair_symbol=pair_symbol,
                exchange_id=exchange_key,
                candles=candles,
                trades_by_minute=trades_by_minute,
                funding_by_minute=funding_by_minute,
                liq_by_minute=liq_by_minute,
            )
            min_ts = min(int(row["ts_ms"]) for row in rows) if rows else None
            max_ts = max(int(row["ts_ms"]) for row in rows) if rows else None
            hours = 0.0
            if min_ts is not None and max_ts is not None and max_ts > min_ts:
                hours = (max_ts - min_ts) / 3_600_000

            features_available = {
                "ohlcv": True,
                "trades": bool(trades_by_minute),
                "funding": bool(funding_by_minute),
                "liquidations": bool(liq_by_minute),
            }
            optional_missing = [name for name in ("trades", "funding", "liquidations") if not features_available[name]]
            status = "completed" if not optional_missing else "partial"
            reason = "ok" if status == "completed" else f"missing_optional:{','.join(optional_missing)}"
            return ExchangeFetchOutput(
                exchange_id=exchange_key,
                exchange_name=exchange_name,
                status=status,
                reason=reason,
                rows=rows,
                data_hours=hours,
                features_available=features_available,
                resolved_symbol=symbol,
                started_at_ms=started_at,
                ended_at_ms=int(time.time() * 1000),
            )
        except RuntimeError as exc:
            return ExchangeFetchOutput(
                exchange_id=exchange_key,
                exchange_name=exchange_name,
                status="stopped",
                reason=str(exc),
                rows=[],
                data_hours=0.0,
                features_available={"ohlcv": False, "trades": False, "funding": False, "liquidations": False},
                resolved_symbol=None,
                started_at_ms=started_at,
                ended_at_ms=int(time.time() * 1000),
            )
        except Exception as exc:
            logger.debug("Historic fetch failed for %s: %s", exchange_key, exc)
            return ExchangeFetchOutput(
                exchange_id=exchange_key,
                exchange_name=exchange_name,
                status="failed",
                reason=f"exchange_error:{exc}",
                rows=[],
                data_hours=0.0,
                features_available={"ohlcv": False, "trades": False, "funding": False, "liquidations": False},
                resolved_symbol=None,
                started_at_ms=started_at,
                ended_at_ms=int(time.time() * 1000),
            )
        finally:
            if exchange is not None:
                try:
                    await asyncio.to_thread(exchange.close)
                except Exception:
                    pass

    async def _wait_control(self, control_hook: ControlHook | None) -> None:
        if control_hook is None:
            return
        while True:
            state = await control_hook()
            if state == "run":
                return
            if state == "stop":
                raise RuntimeError("stopped_by_user")
            await asyncio.sleep(0.3)

    def _select_symbol(self, markets: dict[str, Any], pair_symbol: str) -> str | None:
        base = pair_symbol[:-4] if pair_symbol.endswith("USDT") else pair_symbol
        direct = (
            f"{base}/USDT:USDT",
            f"{base}/USDT",
            f"{base}/USDT-PERP",
            f"{base}/USDTM",
            f"{base}USDT",
        )
        for candidate in direct:
            if candidate in markets:
                return candidate
        for symbol, market in markets.items():
            try:
                market_base = str(market.get("base", "")).upper()
                market_quote = str(market.get("quote", "")).upper()
                market_settle = str(market.get("settle", "")).upper()
                is_contract = bool(market.get("contract")) or bool(market.get("swap")) or bool(market.get("future"))
                if market_base == base and market_quote == "USDT" and (market_settle in {"", "USDT"}) and is_contract:
                    return symbol
            except Exception:
                continue
        return None

    async def _fetch_ohlcv_history(
        self,
        *,
        exchange: Any,
        symbol: str,
        since_ms: int,
        until_ms: int,
        control_hook: ControlHook | None,
        log_hook: LogHook | None,
        exchange_id: str,
        enabled: bool,
    ) -> list[list[Any]]:
        if not enabled:
            return []
        out: list[list[Any]] = []
        next_since = max(0, int(since_ms))
        pages = 0
        seen_ts: set[int] = set()
        limit = 1000
        while next_since < until_ms and pages < self._max_ohlcv_pages:
            await self._wait_control(control_hook)
            try:
                batch = await asyncio.to_thread(exchange.fetch_ohlcv, symbol, "1m", next_since, limit)
            except Exception:
                break
            if not batch:
                break
            last_ts = next_since
            accepted = 0
            for row in batch:
                if not isinstance(row, (list, tuple)) or len(row) < 6:
                    continue
                ts_ms = int(row[0] or 0)
                if ts_ms <= 0 or ts_ms >= until_ms:
                    continue
                if ts_ms in seen_ts:
                    continue
                seen_ts.add(ts_ms)
                out.append([ts_ms, float(row[1] or 0.0), float(row[2] or 0.0), float(row[3] or 0.0), float(row[4] or 0.0), float(row[5] or 0.0)])
                accepted += 1
                if ts_ms > last_ts:
                    last_ts = ts_ms
            pages += 1
            if accepted == 0:
                break
            next_since = int(last_ts) + 60_000
            await asyncio.sleep(max(0.01, float(getattr(exchange, "rateLimit", 200)) / 1000.0))
        out.sort(key=lambda item: int(item[0]))
        if log_hook:
            await log_hook(f"{exchange_id}: fetched {len(out)} OHLCV rows ({pages} pages)")
        return out

    async def _fetch_trades_per_minute(
        self,
        *,
        exchange: Any,
        symbol: str,
        since_ms: int,
        until_ms: int,
        control_hook: ControlHook | None,
        log_hook: LogHook | None,
        exchange_id: str,
    ) -> dict[int, tuple[int, float]]:
        out: dict[int, tuple[int, float]] = {}
        pages = 0
        next_since = max(0, int(since_ms))
        while next_since < until_ms and pages < self._max_trades_pages:
            await self._wait_control(control_hook)
            try:
                batch = await asyncio.to_thread(exchange.fetch_trades, symbol, next_since, 1000)
            except Exception:
                break
            if not batch:
                break
            max_ts = next_since
            accepted = 0
            for item in batch:
                try:
                    ts_ms = int(item.get("timestamp") or 0)
                    if ts_ms <= 0 or ts_ms < since_ms or ts_ms >= until_ms:
                        continue
                    qty = float(item.get("amount") or 0.0)
                    minute = (ts_ms // 60_000) * 60_000
                    prev_count, prev_qty = out.get(minute, (0, 0.0))
                    out[minute] = (prev_count + 1, prev_qty + max(0.0, qty))
                    accepted += 1
                    if ts_ms > max_ts:
                        max_ts = ts_ms
                except Exception:
                    continue
            pages += 1
            if accepted == 0:
                break
            next_since = int(max_ts) + 1
            await asyncio.sleep(max(0.01, float(getattr(exchange, "rateLimit", 200)) / 1000.0))
        if log_hook:
            await log_hook(f"{exchange_id}: fetched trade buckets={len(out)} ({pages} pages)")
        return out

    async def _fetch_funding_per_minute(
        self,
        *,
        exchange: Any,
        symbol: str,
        since_ms: int,
        until_ms: int,
        control_hook: ControlHook | None,
        log_hook: LogHook | None,
        exchange_id: str,
    ) -> dict[int, float]:
        events: list[tuple[int, float]] = []
        next_since = max(0, int(since_ms))
        pages = 0
        while next_since < until_ms and pages < self._max_funding_pages:
            await self._wait_control(control_hook)
            try:
                batch = await asyncio.to_thread(exchange.fetch_funding_rate_history, symbol, next_since, 500)
            except Exception:
                break
            if not batch:
                break
            max_ts = next_since
            accepted = 0
            for item in batch:
                try:
                    ts_ms = int(item.get("timestamp") or 0)
                    if ts_ms <= 0 or ts_ms < since_ms or ts_ms >= until_ms:
                        continue
                    rate = float(item.get("fundingRate") or 0.0)
                    events.append((ts_ms, rate))
                    accepted += 1
                    if ts_ms > max_ts:
                        max_ts = ts_ms
                except Exception:
                    continue
            pages += 1
            if accepted == 0:
                break
            next_since = int(max_ts) + 1
            await asyncio.sleep(max(0.01, float(getattr(exchange, "rateLimit", 200)) / 1000.0))
        events.sort(key=lambda item: item[0])
        if not events:
            return {}
        out: dict[int, float] = {}
        idx = 0
        current = events[0][1]
        for minute in range((since_ms // 60_000) * 60_000, ((until_ms + 59_999) // 60_000) * 60_000, 60_000):
            while idx < len(events) and events[idx][0] <= minute:
                current = events[idx][1]
                idx += 1
            out[minute] = current
        if log_hook:
            await log_hook(f"{exchange_id}: fetched funding points={len(events)} ({pages} pages)")
        return out

    async def _fetch_current_funding(self, *, exchange: Any, symbol: str) -> dict[int, float]:
        try:
            payload = await asyncio.to_thread(exchange.fetch_funding_rate, symbol)
        except Exception:
            return {}
        now_minute = int(time.time() * 1000 // 60_000 * 60_000)
        try:
            rate = float(payload.get("fundingRate") or 0.0)
        except Exception:
            rate = 0.0
        return {now_minute: rate}

    async def _fetch_liquidations_per_minute(
        self,
        *,
        exchange: Any,
        symbol: str,
        since_ms: int,
        until_ms: int,
        control_hook: ControlHook | None,
        log_hook: LogHook | None,
        exchange_id: str,
    ) -> dict[int, int]:
        out: dict[int, int] = {}
        next_since = max(0, int(since_ms))
        pages = 0
        while next_since < until_ms and pages < self._max_liq_pages:
            await self._wait_control(control_hook)
            try:
                batch = await asyncio.to_thread(exchange.fetch_liquidations, symbol, next_since, 500)
            except Exception:
                break
            if not batch:
                break
            max_ts = next_since
            accepted = 0
            for item in batch:
                try:
                    ts_ms = int(item.get("timestamp") or 0)
                    if ts_ms <= 0 or ts_ms < since_ms or ts_ms >= until_ms:
                        continue
                    minute = (ts_ms // 60_000) * 60_000
                    out[minute] = int(out.get(minute, 0)) + 1
                    accepted += 1
                    if ts_ms > max_ts:
                        max_ts = ts_ms
                except Exception:
                    continue
            pages += 1
            if accepted == 0:
                break
            next_since = int(max_ts) + 1
            await asyncio.sleep(max(0.01, float(getattr(exchange, "rateLimit", 200)) / 1000.0))
        if log_hook:
            await log_hook(f"{exchange_id}: fetched liquidation buckets={len(out)} ({pages} pages)")
        return out

    def _build_feature_rows(
        self,
        *,
        pair_symbol: str,
        exchange_id: str,
        candles: list[list[Any]],
        trades_by_minute: dict[int, tuple[int, float]],
        funding_by_minute: dict[int, float],
        liq_by_minute: dict[int, int],
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        last_funding = 0.0
        funding_lookup = dict(funding_by_minute)
        for candle in candles:
            ts_ms = int(candle[0])
            minute_ts = (ts_ms // 60_000) * 60_000
            o = float(candle[1])
            h = float(candle[2])
            l = float(candle[3])
            c = float(candle[4])
            v = float(candle[5])
            spread = max(0.0, h - l)
            best_bid = max(0.0, c - spread * 0.5)
            best_ask = c + spread * 0.5
            trade_count, trade_qty = trades_by_minute.get(minute_ts, (0, 0.0))
            if minute_ts in funding_lookup:
                last_funding = float(funding_lookup.get(minute_ts, last_funding))
            liq_count = int(liq_by_minute.get(minute_ts, 0))
            bid_depth = max(0.0, v * 0.5)
            ask_depth = max(0.0, v * 0.5)
            top_bid = max(0.0, v * 0.25)
            top_ask = max(0.0, v * 0.25)
            denom = top_bid + top_ask
            imbalance = ((top_bid - top_ask) / denom) if denom > 0 else 0.0
            row = {
                "schema_version": "v1_historic_exchange",
                "ts_ms": minute_ts,
                "tab_id": "historic_backfill",
                "pair_symbol": pair_symbol.upper(),
                "enabled_exchange_ids": [exchange_id],
                "connected_exchanges": 1,
                "source_exchange_id": exchange_id,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": max(0.0, best_ask - best_bid),
                "mid_price": c,
                "book_bid_depth": bid_depth,
                "book_ask_depth": ask_depth,
                "book_imbalance_top10": imbalance,
                "top10_bid_volume": top_bid,
                "top10_ask_volume": top_ask,
                "trade_rate_10s": float(trade_count) / 6.0,
                "last_trade_price": c,
                "last_trade_qty": max(0.0, trade_qty),
                "last_trade_side": "",
                "liq_events_60s": liq_count,
                "last_liquidation_qty": 0.0,
                "last_liquidation_side": "",
                "mark_price": c,
                "funding_rate": last_funding,
                "last_kline_close": c,
                "last_kline_interval": "1m",
                "ladder_buy_liquidity": bid_depth,
                "ladder_sell_liquidity": ask_depth,
                "ladder_buy_pct": 50.0,
                "ladder_sell_pct": 50.0,
                "nearest_bid_wall_distance_pct": 0.0,
                "nearest_ask_wall_distance_pct": 0.0,
                "book_synced": True,
                "sync_failures": 0,
                "event_count": int(max(1.0, v)),
                "last_update_id": minute_ts,
            }
            out.append(row)
        return out
