from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.core.time_utils import utc_now_iso


TOP20_EXCHANGES: tuple[tuple[str, str, bool], ...] = (
    ("binance", "Binance (Futures)", True),
    ("bitmart", "BitMart Futures", True),
    ("bybit", "Bybit (Futures)", True),
    ("coinw", "CoinW (Futures)", True),
    ("mexc", "MEXC (Futures)", True),
    ("gate", "Gate (Futures)", True),
    ("bydfi", "BYDFi (Futures)", True),
    ("hyperliquid", "Hyperliquid (Futures)", True),
    ("weex", "WEEX (Futures)", True),
    ("lbank", "LBank (Futures)", True),
    ("okx", "OKX (Futures)", True),
    ("bitget", "Bitget Futures", True),
    ("kucoin", "KuCoin Futures", True),
    ("xt", "XT.COM (Derivatives)", True),
    ("htx", "HTX Futures", True),
    ("whitebit", "WhiteBIT Futures", True),
    ("coinup", "CoinUp.io (Futures)", True),
    ("orangex", "OrangeX Futures", True),
    ("bingx", "BingX (Futures)", True),
    ("toobit", "Toobit Futures", True),
)


@dataclass(slots=True)
class ExchangeStatus:
    exchange_id: str
    name: str
    implemented: bool
    enabled_by_user: bool = True
    supports_pair: bool = False
    connected: bool = False
    reason: str = "adapter_not_implemented"
    last_update_monotonic: float = 0.0
    feature_activity: dict[str, bool] = field(
        default_factory=lambda: {
            "book": False,
            "trades": False,
            "candles": False,
            "mark_funding": False,
            "liquidations": False,
        }
    )


class SourceCoverageManager:
    def __init__(self, selection_path: Path | None = None) -> None:
        self.active_symbol = "BTCUSDT"
        self._selection_path = selection_path
        self._statuses: dict[str, ExchangeStatus] = {
            exchange_id: ExchangeStatus(exchange_id=exchange_id, name=name, implemented=implemented)
            for exchange_id, name, implemented in TOP20_EXCHANGES
        }
        self._apply_loaded_selection()
        self._last_resolution: dict[str, Any] = {
            "type": "symbol_resolution",
            "requested_symbol": self.active_symbol,
            "normalized_symbol": self.active_symbol,
            "scope": "USDT_PERPETUAL",
            "found_exchanges": [],
            "rejected_exchanges": [],
            "resolved_at": utc_now_iso(),
        }

    def _all_exchange_ids(self) -> list[str]:
        return [exchange_id for exchange_id, _, _ in TOP20_EXCHANGES]

    def _load_enabled_exchange_ids(self) -> set[str]:
        all_ids = set(self._all_exchange_ids())
        default_ids = {"binance"} if "binance" in all_ids else set()
        if self._selection_path is None or not self._selection_path.exists():
            return default_ids
        try:
            data = json.loads(self._selection_path.read_text(encoding="utf-8"))
        except Exception:
            return default_ids
        raw = data.get("enabled_exchange_ids", [])
        if not isinstance(raw, list):
            return default_ids
        if len(raw) == 0:
            return default_ids
        selected = {str(item).strip().lower() for item in raw if str(item).strip()}
        if not selected:
            return default_ids
        filtered = {exchange_id for exchange_id in selected if exchange_id in all_ids}
        return filtered or default_ids

    def _write_enabled_exchange_ids(self) -> None:
        if self._selection_path is None:
            return
        enabled = self.enabled_exchange_ids()
        payload = {"enabled_exchange_ids": enabled}
        self._selection_path.parent.mkdir(parents=True, exist_ok=True)
        self._selection_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _apply_loaded_selection(self) -> None:
        selected = self._load_enabled_exchange_ids()
        for exchange_id, status in self._statuses.items():
            status.enabled_by_user = exchange_id in selected

    def enabled_exchange_ids(self) -> list[str]:
        ordered = [exchange_id for exchange_id, _, _ in TOP20_EXCHANGES]
        return [exchange_id for exchange_id in ordered if self._statuses[exchange_id].enabled_by_user]

    def is_exchange_enabled(self, exchange_id: str) -> bool:
        status = self._statuses.get(exchange_id)
        return bool(status and status.enabled_by_user)

    def set_enabled_exchanges(self, exchange_ids: list[str]) -> set[str]:
        all_ids = set(self._all_exchange_ids())
        selected = {str(item).strip().lower() for item in exchange_ids if str(item).strip()}
        selected = {exchange_id for exchange_id in selected if exchange_id in all_ids}
        for status in self._statuses.values():
            status.enabled_by_user = status.exchange_id in selected
            status.connected = False
            status.last_update_monotonic = 0.0
            status.feature_activity = {
                "book": False,
                "trades": False,
                "candles": False,
                "mark_funding": False,
                "liquidations": False,
            }
            if not status.enabled_by_user:
                status.reason = "disabled_by_user"
            elif not status.implemented:
                status.reason = "adapter_not_implemented"
            elif status.supports_pair:
                status.reason = "ready"
            else:
                status.reason = "pair_not_supported"
        self._write_enabled_exchange_ids()
        return selected

    def resolve_symbol(self, symbol: str, binance_symbols: list[str]) -> dict[str, Any]:
        normalized = symbol.strip().upper()
        self.active_symbol = normalized
        found_exchanges: list[str] = []
        rejected_exchanges: list[dict[str, str]] = []
        for status in self._statuses.values():
            status.connected = False
            status.last_update_monotonic = 0.0
            status.feature_activity = {
                "book": False,
                "trades": False,
                "candles": False,
                "mark_funding": False,
                "liquidations": False,
            }
            if not status.enabled_by_user:
                if not status.implemented:
                    status.supports_pair = False
                elif status.exchange_id == "binance":
                    status.supports_pair = normalized in binance_symbols
                else:
                    status.supports_pair = True
                status.reason = "disabled_by_user"
                continue
            if not status.implemented:
                status.supports_pair = False
                status.reason = "adapter_not_implemented"
                rejected_exchanges.append({"exchange_id": status.exchange_id, "reason": status.reason})
                continue
            if status.exchange_id == "binance":
                supported = normalized in binance_symbols
                status.supports_pair = supported
                if supported:
                    status.reason = "ready"
                    found_exchanges.append(status.exchange_id)
                else:
                    status.reason = "pair_not_supported"
                    rejected_exchanges.append({"exchange_id": status.exchange_id, "reason": status.reason})
                continue
            status.supports_pair = True
            status.reason = "ready"
            found_exchanges.append(status.exchange_id)

        self._last_resolution = {
            "type": "symbol_resolution",
            "requested_symbol": normalized,
            "normalized_symbol": normalized,
            "scope": "USDT_PERPETUAL",
            "found_exchanges": found_exchanges,
            "rejected_exchanges": rejected_exchanges,
            "resolved_at": utc_now_iso(),
        }
        return self._last_resolution

    def mark_event(self, exchange_id: str, stream: str) -> None:
        status = self._statuses.get(exchange_id)
        if not status:
            return
        if not status.enabled_by_user:
            status.connected = False
            status.reason = "disabled_by_user"
            return
        if not status.supports_pair:
            return
        status.connected = True
        status.last_update_monotonic = time.monotonic()
        stream = stream.lower()
        if "@depth" in stream or "@bookticker" in stream:
            status.feature_activity["book"] = True
        if "@aggtrade" in stream or "@trade" in stream:
            status.feature_activity["trades"] = True
        if "@kline_" in stream:
            status.feature_activity["candles"] = True
        if "@markprice" in stream:
            status.feature_activity["mark_funding"] = True
        if "@forceorder" in stream:
            status.feature_activity["liquidations"] = True

    def mark_poll_error(self, exchange_id: str, reason: str) -> None:
        status = self._statuses.get(exchange_id)
        if not status:
            return
        if not status.enabled_by_user:
            status.reason = "disabled_by_user"
            return
        if reason == "pair_not_supported":
            status.supports_pair = False
        if not status.connected:
            status.reason = reason

    def source_coverage_payload(self) -> dict[str, Any]:
        now = time.monotonic()
        statuses: list[dict[str, Any]] = []
        feature_counts = {
            "book": 0,
            "trades": 0,
            "candles": 0,
            "mark_funding": 0,
            "liquidations": 0,
        }
        connected_count = 0
        ordered = [self._statuses[exchange_id] for exchange_id, _, _ in TOP20_EXCHANGES]
        for status in ordered:
            is_live = status.enabled_by_user and status.connected and (now - status.last_update_monotonic <= 45.0)
            if is_live:
                connected_count += 1
            for feature_name, active in status.feature_activity.items():
                if is_live and active:
                    feature_counts[feature_name] += 1
            status_reason = status.reason
            if not status.enabled_by_user:
                status_reason = "disabled_by_user"
            statuses.append(
                {
                    "exchange_id": status.exchange_id,
                    "name": status.name,
                    "implemented": status.implemented,
                    "enabled_by_user": status.enabled_by_user,
                    "supports_pair": status.supports_pair,
                    "connected": is_live,
                    "reason": status_reason,
                    "last_update_age_sec": (now - status.last_update_monotonic) if status.last_update_monotonic else None,
                    "features": status.feature_activity,
                }
            )
        return {
            "type": "source_coverage_update",
            "symbol": self.active_symbol,
            "scope": "USDT_PERPETUAL",
            "total_catalog_exchanges": len(TOP20_EXCHANGES),
            "supported_exchanges": sum(1 for status in self._statuses.values() if status.supports_pair),
            "enabled_exchanges": sum(1 for status in self._statuses.values() if status.enabled_by_user),
            "enabled_exchange_ids": self.enabled_exchange_ids(),
            "connected_exchanges": connected_count,
            "feature_contributors": feature_counts,
            "exchanges": statuses,
            "updated_at": utc_now_iso(),
        }

    @property
    def last_resolution(self) -> dict[str, Any]:
        return self._last_resolution
