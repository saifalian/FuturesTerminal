from __future__ import annotations

import asyncio
import hashlib
import ctypes
import gc
import hashlib
import json
import logging
import math
import multiprocessing as mp
import os
import random
import sqlite3
import shutil
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

try:
    import numpy as np
except Exception:
    np = None
try:
    import pyarrow.parquet as pq
except Exception:
    pq = None

from app.api.ws_server import BroadcastHub
from app.core.time_utils import utc_now_iso
from app.services.ml.historic_fetcher import MlHistoricFetcher
from app.services.ml.repository import MAX_HISTORIC_DATA_DAYS, MlProfile, MlRepository
from app.services.tool_mode_matrix import FEATURE_COLUMNS_BY_TOOL, event_stream_to_tool_id, is_tool_enabled

logger = logging.getLogger(__name__)

os.environ.setdefault("PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True")

FEATURE_COLUMNS = [
    "spread",
    "mid_price",
    "book_bid_depth",
    "book_ask_depth",
    "book_imbalance_top10",
    "top10_bid_volume",
    "top10_ask_volume",
    "trade_rate_10s",
    "liq_events_60s",
    "liq_count_60s",
    "liq_notional_60s",
    "liq_buy_sell_imbalance",
    "liq_momentum",
    "funding_rate",
    "ladder_buy_pct",
    "ladder_sell_pct",
    "nearest_bid_wall_distance_pct",
    "nearest_ask_wall_distance_pct",
    "return_15s",
    "return_60s",
    "rolling_volatility_30s",
    "vwap_distance_pct",
    "market_buy_volume_10s",
    "market_sell_volume_10s",
    "open_interest_change_pct",
    "return_5s",
    "return_30s",
    "rolling_volatility_60s",
    "candle_range_pct",
    "atr_short",
    "ema_fast_distance_pct",
    "ema_slow_distance_pct",
    "trend_slope_short",
    "aggressive_buy_sell_delta",
    "cancel_rate_orderbook",
    "wall_strength_bid",
    "wall_strength_ask",
    "wall_persistence_seconds",
    "spread_change_rate",
    "volume_10s",
    "volume_30s",
    "relative_volume_ratio",
    "hour_of_day_sin",
    "hour_of_day_cos",
    "session_asia_eu_us",
    "oi_velocity",
    "funding_rate_change",
]


@dataclass(slots=True)
class _RunControl:
    pause: bool = False
    stop: bool = False


@dataclass(slots=True)
class _PaperPosition:
    side: str = ""
    entry_price: float = 0.0
    qty: float = 0.0
    opened_at: int = 0
    peak_price: float = 0.0
    trough_price: float = 0.0
    entry_context: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class _BackfillJob:
    job_id: str
    pair_symbol: str
    status: str
    stage: str
    progress: float
    mode: str
    source_path: str
    target_path: str
    days: int
    started_at: str
    updated_at: str
    ended_at: str | None = None


def _market_chunk_extract_worker(
    file_path: str,
    selected_exchanges: list[str],
    start_ts_ms: int | None,
    end_ts_ms: int | None,
    out_path: str,
) -> dict[str, Any]:
    """Extract filtered events from one parquet chunk in a short-lived subprocess.

    Purpose: Arrow allocator memory is released when process exits.
    """
    try:
        import pyarrow.parquet as _pq  # type: ignore
    except Exception as exc:  # pragma: no cover
        return {"ok": False, "error": f"pyarrow_unavailable:{exc}"}
    selected_set = {str(x).lower() for x in (selected_exchanges or [])}
    src = Path(file_path)
    if not src.exists():
        return {"ok": True, "rows": 0, "events_scanned": 0}
    try:
        pf = _pq.ParquetFile(str(src))
    except Exception as exc:
        return {"ok": False, "error": f"parquet_open_failed:{exc}"}

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    rows_out = 0
    scanned = 0
    worker_peak_rss_bytes = 0
    try:
        import psutil as _worker_psutil  # type: ignore
        _worker_proc = _worker_psutil.Process(os.getpid())
    except Exception:
        _worker_proc = None
    try:
        with out.open("w", encoding="utf-8") as handle:
            for batch in pf.iter_batches(
                columns=[
                    "source_exchange_id",
                    "stream",
                    "event_type",
                    "ts_exchange_ms",
                    "ts_receive_ms",
                    "payload_json",
                ],
                batch_size=25_000,
            ):
                cols = {
                    "source_exchange_id": batch.column(0).to_pylist(),
                    "stream": batch.column(1).to_pylist(),
                    "event_type": batch.column(2).to_pylist(),
                    "ts_exchange_ms": batch.column(3).to_pylist(),
                    "ts_receive_ms": batch.column(4).to_pylist(),
                    "payload_json": batch.column(5).to_pylist(),
                }
                n = len(cols["stream"])
                scanned += int(n)
                if _worker_proc is not None:
                    try:
                        worker_peak_rss_bytes = max(worker_peak_rss_bytes, int(_worker_proc.memory_info().rss))
                    except Exception:
                        pass
                for i in range(n):
                    ex = str(cols["source_exchange_id"][i] or "unknown").lower()
                    if selected_set and ex not in selected_set:
                        continue
                    ts_receive = int(cols["ts_receive_ms"][i] or 0)
                    ts_exchange = int(cols["ts_exchange_ms"][i] or 0)
                    ts_event = ts_receive if ts_receive > 0 else ts_exchange
                    if start_ts_ms is not None and ts_event < start_ts_ms:
                        continue
                    if end_ts_ms is not None and ts_event > end_ts_ms:
                        continue
                    rec = {
                        "source_exchange_id": ex,
                        "stream": str(cols["stream"][i] or ""),
                        "event_type": str(cols["event_type"][i] or ""),
                        "ts_receive_ms": ts_receive,
                        "ts_exchange_ms": ts_exchange,
                        "payload_json": str(cols["payload_json"][i] or "{}"),
                    }
                    handle.write(json.dumps(rec, separators=(",", ":"), ensure_ascii=True))
                    handle.write("\n")
                    rows_out += 1
        return {
            "ok": True,
            "rows": int(rows_out),
            "events_scanned": int(scanned),
            "worker_peak_rss_bytes": int(worker_peak_rss_bytes),
        }
    except Exception as exc:
        return {"ok": False, "error": f"worker_failed:{exc}"}


def _market_chunk_extract_worker_entry(
    queue_obj: Any,
    file_path: str,
    selected_exchanges: list[str],
    start_ts_ms: int | None,
    end_ts_ms: int | None,
    out_path: str,
) -> None:
    res = _market_chunk_extract_worker(
        file_path=file_path,
        selected_exchanges=selected_exchanges,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        out_path=out_path,
    )
    queue_obj.put(res)
    error_text: str = ""
    selected_exchanges: list[str] = field(default_factory=list)
    exchange_progress: list[dict[str, Any]] = field(default_factory=list)
    instance_id: str = ""
    instance_name: str = "Default"


@dataclass(slots=True)
class _MlInstanceView:
    instance_id: str
    name: str
    pair_symbol: str
    created_at: str
    updated_at: str
    archived: bool


@dataclass(slots=True)
class _ReplaySession:
    replay_id: str
    run_id: str
    pair_symbol: str
    instance_id: str
    source_mode: str
    mode: str
    status: str
    cursor: int
    speed: float
    epoch_index: int
    split: str
    created_at: str
    updated_at: str
    frames: list[dict[str, Any]]
    parity_stats: dict[str, Any] = field(default_factory=dict)


class MlRuntimeManager:
    def __init__(
        self,
        *,
        repository: MlRepository,
        tab_sessions: Any,
        hub: BroadcastHub,
        models_root: Path,
        feature_data_root: Path,
        terminal_sqlite_path: Path,
        market_events_root: Path,
        tool_mode_matrix_resolver: Any | None = None,
        workload_controls_resolver: Any | None = None,
    ) -> None:
        self._repository = repository
        self._tab_sessions = tab_sessions
        self._hub = hub
        self._models_root = models_root
        self._feature_data_root = feature_data_root
        self._terminal_sqlite_path = terminal_sqlite_path
        self._market_events_root = market_events_root
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._controls: dict[str, _RunControl] = {}
        self._consumer_task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._active_run_id: str | None = None
        self._queued_ids: set[str] = set()
        self._bot_tasks: dict[str, asyncio.Task] = {}
        self._bot_stop_flags: dict[str, asyncio.Event] = {}
        self._bot_pause_flags: dict[str, bool] = {}
        self._backfill_jobs: dict[str, _BackfillJob] = {}
        self._backfill_logs: dict[str, list[dict[str, str]]] = {}
        self._backfill_controls: dict[str, _RunControl] = {}
        self._backfill_tasks: dict[str, asyncio.Task] = {}
        self._pair_backfill_job: dict[str, str] = {}
        self._instance_backfill_job: dict[str, str] = {}
        self._replay_sessions: dict[str, _ReplaySession] = {}
        self._historic_data_days_max = MAX_HISTORIC_DATA_DAYS
        self._historic_fetcher = MlHistoricFetcher()
        self._tool_mode_matrix_resolver = tool_mode_matrix_resolver
        self._workload_controls_resolver = workload_controls_resolver
        self._epoch_summary_cache: dict[str, dict[str, Any]] = {}
        self._last_run_update_emit_monotonic: dict[str, float] = {}
        self._last_info_log_emit_monotonic: dict[str, float] = {}
        self._dataset_cache_root = self._models_root / "_dataset_cache"

    @staticmethod
    def _safe_div(numerator: float, denominator: float, eps: float = 1e-12) -> float:
        return float(numerator / denominator) if abs(float(denominator)) > eps else 0.0

    @staticmethod
    def _aggressive_buy_sell_delta(buy_volume: float, sell_volume: float) -> float:
        total = float(max(0.0, buy_volume) + max(0.0, sell_volume))
        return MlRuntimeManager._safe_div(float(buy_volume) - float(sell_volume), total)

    def _market_events_feature_cache_key(
        self,
        *,
        pair_symbol: str,
        config: dict[str, Any],
        chunk_rows: list[dict[str, Any]],
        effective_sample_ms: int,
        start_ts_ms: int | None,
        end_ts_ms: int | None,
    ) -> str:
        horizons = list(config.get("horizons", []) or [])
        training_cfg = config.get("training", {}) if isinstance(config.get("training"), dict) else {}
        payload = {
            "pair": str(pair_symbol).upper(),
            "label_price_version": "bookticker_mid_state_v3_strict_grid",
            "target_mode": str(config.get("target_mode", "trade_outcome") or "trade_outcome"),
            "sample_ms": int(effective_sample_ms),
            "strict_grid_enabled": True,
            "max_ffill_gap_ms": int(os.getenv("ML_GRID_MAX_FFILL_GAP_MS", "1500")),
            "stale_trade_ms": int(os.getenv("ML_GRID_STALE_TRADE_MS", "3000")),
            "stale_depth_ms": int(os.getenv("ML_GRID_STALE_DEPTH_MS", "3000")),
            "lookback": int(training_cfg.get("lookback_steps", 60) or 60),
            "horizons": [int(h) for h in horizons],
            "start_ts_ms": int(start_ts_ms or 0),
            "end_ts_ms": int(end_ts_ms or 0),
            "chunk_count": len(chunk_rows),
            "chunk_fp": [
                [
                    str(item.get("file_path", "")),
                    int(item.get("size_bytes", 0) or 0),
                    int(item.get("ts_start_ms", 0) or 0),
                    int(item.get("ts_end_ms", 0) or 0),
                ]
                for item in chunk_rows
            ],
        }
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
        return digest

    async def start(self) -> None:
        if self._consumer_task and not self._consumer_task.done():
            return
        self._models_root.mkdir(parents=True, exist_ok=True)
        self._stop.clear()
        self._consumer_task = asyncio.create_task(self._consume_loop(), name="ml-runtime-consumer")
        await self._recover_incomplete_runs()

    async def stop(self) -> None:
        if self._consumer_task is None and not self._bot_tasks and not self._backfill_tasks:
            return
        self._stop.set()
        for control in self._controls.values():
            control.stop = True
        for event in self._bot_stop_flags.values():
            event.set()
        tasks = [task for task in [self._consumer_task, *self._bot_tasks.values()] if task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._consumer_task = None
        self._bot_tasks.clear()
        self._bot_stop_flags.clear()
        self._bot_pause_flags.clear()
        for control in self._backfill_controls.values():
            control.stop = True
        backfill_tasks = list(self._backfill_tasks.values())
        for task in backfill_tasks:
            task.cancel()
        if backfill_tasks:
            await asyncio.gather(*backfill_tasks, return_exceptions=True)
        self._backfill_tasks.clear()
        self._backfill_controls.clear()
        self._replay_sessions.clear()

    async def close_all_replays(self) -> None:
        self._replay_sessions.clear()

    def _tool_matrix(self) -> dict[str, Any]:
        resolver = self._tool_mode_matrix_resolver
        if callable(resolver):
            try:
                value = resolver()
                if isinstance(value, dict):
                    return value
            except Exception:
                return {}
        return {}

    def _mode_tool_enabled(self, mode_id: str, tool_id: str) -> bool:
        return is_tool_enabled(self._tool_matrix(), mode_id=mode_id, tool_id=tool_id)

    def _workload_controls(self) -> dict[str, Any]:
        resolver = self._workload_controls_resolver
        if callable(resolver):
            try:
                value = resolver()
                if isinstance(value, dict):
                    return value
            except Exception:
                return {}
        return {}

    async def _recover_incomplete_runs(self) -> None:
        runs = await self._repository.list_runs(limit=200)
        for run in runs:
            if run["status"] in {"RUNNING", "QUEUED"}:
                await self._repository.update_run(
                    run["run_id"],
                    {
                        "status": "PAUSED",
                        "updated_at": utc_now_iso(),
                        "error_text": "Recovered after restart; press Start to resume.",
                    },
                )

    async def list_pairs(self) -> list[str]:
        active_pairs = await self._tab_sessions.list_pairs()
        profile_pairs = await self._repository.list_profile_pairs()
        merged = sorted({item.upper() for item in [*active_pairs, *profile_pairs] if item})
        return merged

    async def list_instances(self, *, lite: bool = False) -> list[dict[str, Any]]:
        if not lite:
            for pair in await self.list_pairs():
                await self._default_instance(pair)
        items = await self._repository.list_instances(include_archived=False)
        result: list[dict[str, Any]] = []
        for item in items:
            try:
                if lite:
                    result.append(
                        {
                            "instance_id": item.instance_id,
                            "name": item.name,
                            "pair_symbol": item.pair_symbol,
                            "created_at": item.created_at,
                            "updated_at": item.updated_at,
                            "archived": item.archived,
                            "selected_exchange_count": 0,
                            "latest_approved_metrics": {},
                            "bot_status": "IDLE",
                        }
                    )
                    continue
                profile = await self._repository.get_instance_profile(item.instance_id, item.pair_symbol)
                current = await self._repository.current_model(item.pair_symbol, item.instance_id)
                bot = await self._repository.bot_state(item.pair_symbol, item.instance_id)
                result.append(
                    {
                        "instance_id": item.instance_id,
                        "name": item.name,
                        "pair_symbol": item.pair_symbol,
                        "created_at": item.created_at,
                        "updated_at": item.updated_at,
                        "archived": item.archived,
                        "selected_exchange_count": len(profile.training_selected_exchanges or profile.selected_exchanges),
                        "latest_approved_metrics": current.get("metrics", {}) if current else {},
                        "bot_status": (bot or {}).get("status", "IDLE"),
                    }
                )
            except Exception as exc:
                logger.warning("Skipping broken ML instance %s (%s): %s", item.instance_id, item.name, exc)
                continue
        return result

    async def create_instance(self, pair_symbol: str, name: str) -> dict[str, Any]:
        pair = pair_symbol.upper().strip()
        if not pair:
            raise ValueError("pair_symbol is required.")
        clean_name = (name or "").strip() or f"{pair} Bot"
        instance_id = f"inst_{int(time.time())}_{uuid4().hex[:8]}"
        item = await self._repository.create_instance(instance_id=instance_id, name=clean_name, pair_symbol=pair)
        profile = await self._repository.get_profile(pair)
        await self._repository.upsert_instance_profile(instance_id, pair, self._profile_to_dict(profile))
        await self._repository.upsert_profile(pair, self._profile_to_dict(profile))
        await self._write_pair_settings_file(pair, self._profile_to_dict(profile), instance_name=clean_name, instance_id=instance_id)
        return {
            "instance_id": item.instance_id,
            "name": item.name,
            "pair_symbol": item.pair_symbol,
            "created_at": item.created_at,
            "updated_at": item.updated_at,
            "archived": item.archived,
        }

    async def update_instance(self, instance_id: str, *, name: str | None = None, archived: bool | None = None) -> dict[str, Any]:
        current = await self._repository.get_instance(instance_id)
        if current is None:
            raise ValueError("Instance not found.")
        updated = await self._repository.update_instance(instance_id, name=name, archived=archived)
        if updated is None:
            raise ValueError("Instance not found.")
        if name and name.strip() and name.strip() != current.name:
            await self._rename_instance_folder_prefix(current.pair_symbol, current.name, updated.name, instance_id)
        return {
            "instance_id": updated.instance_id,
            "name": updated.name,
            "pair_symbol": updated.pair_symbol,
            "created_at": updated.created_at,
            "updated_at": updated.updated_at,
            "archived": updated.archived,
        }

    async def archive_instance(self, instance_id: str) -> dict[str, Any]:
        return await self.update_instance(instance_id, archived=True)

    async def _default_instance(self, pair_symbol: str) -> dict[str, Any]:
        pair = pair_symbol.upper()
        item = await self._repository.ensure_default_instance_for_pair(pair)
        base_profile = await self._repository.get_profile(pair)
        profile = await self._repository.get_instance_profile(item.instance_id, pair)
        if not (profile.training_selected_exchanges or profile.selected_exchanges) and (
            (base_profile.training_selected_exchanges or base_profile.selected_exchanges)
            or bool(base_profile.use_historic_data)
            or not bool(base_profile.use_local_data)
        ):
            await self._repository.upsert_instance_profile(item.instance_id, pair, self._profile_to_dict(base_profile))
            profile = await self._repository.get_instance_profile(item.instance_id, pair)
        if not (profile.training_selected_exchanges or profile.selected_exchanges):
            defaults = await self._default_selected_exchanges(pair)
            payload = self._profile_to_dict(profile)
            payload["selected_exchanges"] = defaults
            payload["training_selected_exchanges"] = list(defaults)
            payload["bot_data_selected_exchanges"] = list(defaults)
            payload["bot_execution_selected_exchanges"] = list(defaults)
            await self._repository.upsert_instance_profile(item.instance_id, pair, payload)
        return {
            "instance_id": item.instance_id,
            "name": item.name,
            "pair_symbol": item.pair_symbol,
            "created_at": item.created_at,
            "updated_at": item.updated_at,
            "archived": item.archived,
        }

    async def get_profile_by_instance(self, instance_id: str) -> dict[str, Any]:
        item = await self._repository.get_instance(instance_id)
        if item is None:
            raise ValueError("Instance not found.")
        profile = await self._repository.get_instance_profile(instance_id, item.pair_symbol)
        payload = self._profile_to_dict(profile)
        if not self._training_exchanges(payload):
            defaults = await self._default_selected_exchanges(item.pair_symbol)
            payload["selected_exchanges"] = list(defaults)
            payload["training_selected_exchanges"] = list(defaults)
            payload["bot_data_selected_exchanges"] = list(defaults)
            payload["bot_execution_selected_exchanges"] = list(defaults)
        payload["instance_id"] = instance_id
        payload["instance_name"] = item.name
        return payload

    async def save_profile_by_instance(self, instance_id: str, config: dict[str, Any]) -> dict[str, Any]:
        item = await self._repository.get_instance(instance_id)
        if item is None:
            raise ValueError("Instance not found.")
        profile = await self._repository.upsert_instance_profile(instance_id, item.pair_symbol, config)
        await self._repository.upsert_profile(item.pair_symbol, config)
        payload = self._profile_to_dict(profile)
        if not self._training_exchanges(payload):
            defaults = await self._default_selected_exchanges(item.pair_symbol)
            payload["selected_exchanges"] = list(defaults)
            payload["training_selected_exchanges"] = list(defaults)
            payload["bot_data_selected_exchanges"] = list(defaults)
            payload["bot_execution_selected_exchanges"] = list(defaults)
            await self._repository.upsert_instance_profile(instance_id, item.pair_symbol, payload)
        await self._write_pair_settings_file(item.pair_symbol, payload, instance_name=item.name, instance_id=instance_id)
        payload["instance_id"] = instance_id
        payload["instance_name"] = item.name
        return payload

    async def data_status_by_instance(self, instance_id: str) -> dict[str, Any]:
        return await self.data_status_by_instance_for_source(instance_id, dataset_source="features_manifest")

    async def data_status_by_instance_for_source(self, instance_id: str, dataset_source: str = "features_manifest") -> dict[str, Any]:
        item = await self._repository.get_instance(instance_id)
        if item is None:
            raise ValueError("Instance not found.")
        source_mode = self._normalize_dataset_source(dataset_source)
        if source_mode == "local_market_events":
            market_status = await self._market_events_data_status(item.pair_symbol)
            return {
                **market_status,
                "pair_symbol": item.pair_symbol,
                "instance_id": instance_id,
                "instance_name": item.name,
                "selected_source": "local_market_events",
                "selected_status": market_status,
            }
        profile = await self.get_profile_by_instance(instance_id)
        live_status = await self._repository.data_status(item.pair_symbol)
        historic_status = await self._historic_data_status(item.pair_symbol, item.name, instance_id)
        use_local = bool(profile.get("use_local_data", True))
        use_historic = bool(profile.get("use_historic_data", False))
        statuses: list[dict[str, Any]] = []
        if use_local:
            statuses.append(live_status)
        if use_historic:
            statuses.append(historic_status)
        if not statuses:
            active = {
                "pair_symbol": item.pair_symbol,
                "file_count": 0,
                "rows_total": 0,
                "min_ts_ms": None,
                "max_ts_ms": None,
                "data_hours": 0.0,
            }
            active_source = "none"
            active_source_path = "-"
        elif len(statuses) == 1:
            active = statuses[0]
            active_source = "live_logger" if use_local else "historic_data_acquired"
            active_source_path = str(self._feature_data_root / item.pair_symbol) if use_local else str(
                self._historic_data_root(item.pair_symbol, item.name, instance_id)
            )
        else:
            min_candidates = [int(s["min_ts_ms"]) for s in statuses if s.get("min_ts_ms") is not None]
            max_candidates = [int(s["max_ts_ms"]) for s in statuses if s.get("max_ts_ms") is not None]
            min_ts = min(min_candidates) if min_candidates else None
            max_ts = max(max_candidates) if max_candidates else None
            hours = 0.0
            if min_ts is not None and max_ts is not None and max_ts > min_ts:
                hours = (max_ts - min_ts) / 3_600_000
            active = {
                "pair_symbol": item.pair_symbol,
                "file_count": int(sum(int(s.get("file_count", 0) or 0) for s in statuses)),
                "rows_total": int(sum(int(s.get("rows_total", 0) or 0) for s in statuses)),
                "min_ts_ms": min_ts,
                "max_ts_ms": max_ts,
                "data_hours": hours,
            }
            active_source = "live+historic"
            active_source_path = (
                f"{self._feature_data_root / item.pair_symbol} | "
                f"{self._historic_data_root(item.pair_symbol, item.name, instance_id)}"
            )
        return {
            **active,
            "pair_symbol": item.pair_symbol,
            "instance_id": instance_id,
            "instance_name": item.name,
            "active_source": active_source,
            "active_source_path": active_source_path,
            "live_status": live_status,
            "historic_status": historic_status,
            "use_local_data": use_local,
            "use_historic_data": use_historic,
            "historic_data_days": int(profile.get("historic_data_days", 90)),
            "selected_source": "features_manifest",
            "selected_status": active,
        }

    async def get_profile(self, pair_symbol: str) -> dict[str, Any]:
        default_instance = await self._default_instance(pair_symbol.upper())
        return await self.get_profile_by_instance(default_instance["instance_id"])

    async def save_profile(self, pair_symbol: str, config: dict[str, Any]) -> dict[str, Any]:
        default_instance = await self._default_instance(pair_symbol.upper())
        return await self.save_profile_by_instance(default_instance["instance_id"], config)

    async def apply_recommended_gates(self, run_id: str) -> dict[str, Any]:
        run = await self._repository.get_run(run_id)
        if run is None:
            raise ValueError("Run not found.")
        pair_symbol = str((run.get("pair_symbol") if isinstance(run, dict) else getattr(run, "pair_symbol", "")) or "").upper()
        if not pair_symbol:
            raise ValueError("Run has no pair symbol.")
        metrics = dict((run.get("metrics", {}) if isinstance(run, dict) else getattr(run, "metrics", {})) or {})
        try:
            conf = float(metrics.get("recommended_confidence_threshold"))
            qual = float(metrics.get("recommended_quality_threshold"))
            gap = float(metrics.get("recommended_confidence_gap"))
        except Exception as exc:
            raise ValueError("No recommended gate thresholds found for this run. Run a captured validation trace first.") from exc

        profile = await self.get_profile(pair_symbol)
        paper_bot = dict(profile.get("paper_bot", {}))
        paper_bot["confidence_threshold"] = max(0.0, min(1.0, conf))
        paper_bot["quality_threshold"] = max(0.0, min(1.0, qual))
        paper_bot["min_confidence_gap"] = max(0.0, min(1.0, gap))
        profile["paper_bot"] = paper_bot
        saved = await self.save_profile(pair_symbol, profile)
        return {
            "ok": True,
            "run_id": run_id,
            "pair_symbol": pair_symbol,
            "applied": {
                "confidence_threshold": paper_bot["confidence_threshold"],
                "quality_threshold": paper_bot["quality_threshold"],
                "min_confidence_gap": paper_bot["min_confidence_gap"],
            },
            "profile": saved,
        }

    async def data_status(self, pair_symbol: str) -> dict[str, Any]:
        return await self.data_status_for_source(pair_symbol, dataset_source="features_manifest")

    async def data_status_for_source(self, pair_symbol: str, dataset_source: str = "features_manifest") -> dict[str, Any]:
        pair_symbol = pair_symbol.upper()
        source_mode = self._normalize_dataset_source(dataset_source)
        if source_mode == "local_market_events":
            market_status = await self._market_events_data_status(pair_symbol)
            return {
                **market_status,
                "pair_symbol": pair_symbol,
                "selected_source": "local_market_events",
                "selected_status": market_status,
            }
        profile = await self.get_profile(pair_symbol)
        live_status = await self._repository.data_status(pair_symbol)
        historic_status = await self._historic_data_status(pair_symbol)
        use_local = bool(profile.get("use_local_data", True))
        use_historic = bool(profile.get("use_historic_data", False))
        statuses: list[dict[str, Any]] = []
        if use_local:
            statuses.append(live_status)
        if use_historic:
            statuses.append(historic_status)
        if not statuses:
            active = {
                "pair_symbol": pair_symbol,
                "file_count": 0,
                "rows_total": 0,
                "min_ts_ms": None,
                "max_ts_ms": None,
                "data_hours": 0.0,
            }
            active_source = "none"
            active_source_path = "-"
        elif len(statuses) == 1:
            active = statuses[0]
            active_source = "live_logger" if use_local else "historic_data_acquired"
            active_source_path = str(self._feature_data_root / pair_symbol) if use_local else str(self._historic_data_root(pair_symbol))
        else:
            min_candidates = [int(item["min_ts_ms"]) for item in statuses if item.get("min_ts_ms") is not None]
            max_candidates = [int(item["max_ts_ms"]) for item in statuses if item.get("max_ts_ms") is not None]
            min_ts = min(min_candidates) if min_candidates else None
            max_ts = max(max_candidates) if max_candidates else None
            hours = 0.0
            if min_ts is not None and max_ts is not None and max_ts > min_ts:
                hours = (max_ts - min_ts) / 3_600_000
            active = {
                "pair_symbol": pair_symbol,
                "file_count": int(sum(int(item.get("file_count", 0) or 0) for item in statuses)),
                "rows_total": int(sum(int(item.get("rows_total", 0) or 0) for item in statuses)),
                "min_ts_ms": min_ts,
                "max_ts_ms": max_ts,
                "data_hours": hours,
            }
            active_source = "live+historic"
            active_source_path = f"{self._feature_data_root / pair_symbol} | {self._historic_data_root(pair_symbol)}"
        return {
            **active,
            "pair_symbol": pair_symbol,
            "active_source": active_source,
            "active_source_path": active_source_path,
            "live_status": live_status,
            "historic_status": historic_status,
            "use_local_data": use_local,
            "use_historic_data": use_historic,
            "historic_data_days": int(profile.get("historic_data_days", 90)),
            "selected_source": "features_manifest",
            "selected_status": active,
        }

    @staticmethod
    def _normalize_dataset_source(dataset_source: str | None) -> str:
        source_mode = str(dataset_source or "features_manifest").strip().lower()
        if source_mode not in {"features_manifest", "local_market_events"}:
            source_mode = "features_manifest"
        return source_mode

    async def start_run(self, pair_symbol: str, dataset_source: str = "features_manifest") -> dict[str, Any]:
        default_instance = await self._default_instance(pair_symbol.upper())
        return await self.start_run_by_instance(default_instance["instance_id"], dataset_source=dataset_source)

    async def start_run_by_instance(self, instance_id: str, dataset_source: str = "features_manifest") -> dict[str, Any]:
        item = await self._repository.get_instance(instance_id)
        if item is None:
            raise ValueError("Instance not found.")
        pair_symbol = item.pair_symbol
        profile_payload = await self.get_profile_by_instance(instance_id)
        if not self._training_exchanges(profile_payload):
            raise ValueError("No training exchanges selected for this pair.")

        existing_runs = await self._repository.list_runs(instance_id=instance_id, limit=20)
        paused = next((run for run in existing_runs if run["status"] == "PAUSED"), None)
        if paused:
            run_id = paused["run_id"]
            latest_controls = self._workload_controls()
            latest_ram = self._normalize_training_ram_budget_gb(latest_controls.get("training_ram_budget_gb"))
            latest_prefetch = bool(latest_controls.get("training_prefetch_enabled", False))
            latest_chunk_group_size = latest_controls.get("training_chunk_group_size", "auto")
            latest_recycle_enabled = bool(latest_controls.get("training_process_recycle_enabled", True))
            latest_groups_before_restart = int(max(1, int(latest_controls.get("training_worker_groups_before_restart", 1) or 1)))
            latest_worker_mem_cap = self._normalize_training_ram_budget_gb(latest_controls.get("training_worker_memory_cap_gb"))
            latest_keep_shards = bool(latest_controls.get("keep_training_shards", False))
            paused_cfg = dict(paused.get("config", {}) if isinstance(paused.get("config"), dict) else {})
            paused_cfg["training_ram_budget_gb"] = latest_ram
            paused_cfg["training_prefetch_enabled"] = latest_prefetch
            paused_cfg["training_chunk_group_size"] = latest_chunk_group_size
            paused_cfg["training_process_recycle_enabled"] = latest_recycle_enabled
            paused_cfg["training_worker_groups_before_restart"] = latest_groups_before_restart
            paused_cfg["training_worker_memory_cap_gb"] = latest_worker_mem_cap
            paused_cfg["keep_training_shards"] = latest_keep_shards
            self._controls.setdefault(run_id, _RunControl()).pause = False
            await self._repository.update_run(
                run_id,
                {
                    "status": "QUEUED",
                    "error_text": "",
                    "config": paused_cfg,
                    "updated_at": utc_now_iso(),
                },
            )
            await self._emit_run_log(
                run_id,
                "INFO",
                (
                    "Resuming paused run with refreshed workload controls: "
                    f"training_ram_budget_gb={latest_ram if latest_ram is not None else 'auto'}, "
                    f"training_prefetch_enabled={latest_prefetch}, "
                    f"training_chunk_group_size={latest_chunk_group_size}, "
                    f"training_process_recycle_enabled={latest_recycle_enabled}, "
                    f"training_worker_groups_before_restart={latest_groups_before_restart}, "
                    f"training_worker_memory_cap_gb={latest_worker_mem_cap if latest_worker_mem_cap is not None else 'auto'}, "
                    f"keep_training_shards={latest_keep_shards}"
                ),
            )
            await self._emit_run_update(run_id)
            await self._enqueue_run(run_id)
            return {"ok": True, "run_id": run_id, "resumed": True}

        # Safety: stop existing queued/running runs for the same instance before queuing a new one.
        # This avoids overlapping debug/training sessions and keeps label-debug runs deterministic.
        for existing in existing_runs:
            existing_id = str(existing.get("run_id", "") or "")
            status = str(existing.get("status", "") or "").upper()
            if not existing_id:
                continue
            if status in {"QUEUED", "RUNNING", "PAUSED"}:
                await self.stop_run(existing_id)

        run_id = f"run_{int(time.time())}_{uuid4().hex[:8]}"
        now = utc_now_iso()
        source_mode = str(dataset_source or "features_manifest").strip().lower()
        if source_mode not in {"features_manifest", "local_market_events"}:
            source_mode = "features_manifest"
        run_config = dict(profile_payload)
        run_config["dataset_source"] = source_mode
        workload_controls = self._workload_controls()
        run_config["training_ram_budget_gb"] = self._normalize_training_ram_budget_gb(
            workload_controls.get("training_ram_budget_gb")
        )
        run_config["training_prefetch_enabled"] = bool(workload_controls.get("training_prefetch_enabled", False))
        run_config["training_chunk_group_size"] = workload_controls.get("training_chunk_group_size", "auto")
        run_config["training_process_recycle_enabled"] = bool(workload_controls.get("training_process_recycle_enabled", True))
        run_config["training_worker_groups_before_restart"] = int(max(1, int(workload_controls.get("training_worker_groups_before_restart", 1) or 1)))
        run_config["training_worker_memory_cap_gb"] = self._normalize_training_ram_budget_gb(workload_controls.get("training_worker_memory_cap_gb"))
        run_config["keep_training_shards"] = bool(workload_controls.get("keep_training_shards", False))
        run = {
            "run_id": run_id,
            "pair_symbol": pair_symbol,
            "instance_id": instance_id,
            "status": "QUEUED",
            "stage": "queued",
            "stage_progress": 0.0,
            "device": "pending",
            "created_at": now,
            "updated_at": now,
            "started_at": None,
            "ended_at": None,
            "config": run_config,
            "metrics": {},
            "error_text": "",
        }
        await self._repository.create_run(run)
        self._controls[run_id] = _RunControl()
        await self._emit_run_log(run_id, "INFO", "Run created and queued.")
        await self._emit_run_update(run_id)
        await self._enqueue_run(run_id)
        return {"ok": True, "run_id": run_id, "resumed": False}

    async def pause_run(self, run_id: str) -> dict[str, Any]:
        control = self._controls.setdefault(run_id, _RunControl())
        control.pause = True
        await self._repository.update_run(
            run_id,
            {
                "status": "PAUSED",
                "updated_at": utc_now_iso(),
            },
        )
        await self._emit_run_log(run_id, "INFO", "Pause requested.")
        await self._emit_run_update(run_id)
        return {"ok": True, "run_id": run_id}

    async def stop_run(self, run_id: str) -> dict[str, Any]:
        control = self._controls.setdefault(run_id, _RunControl())
        control.stop = True
        run = await self._repository.get_run(run_id)
        should_mark_stopped = False
        if run_id in self._queued_ids:
            self._queued_ids.discard(run_id)
            should_mark_stopped = True
        if run is not None and str(run.get("status", "")).upper() == "QUEUED":
            should_mark_stopped = True
        if should_mark_stopped:
            await self._repository.update_run(
                run_id,
                {
                    "status": "STOPPED",
                    "stage": "stopped",
                    "stage_progress": 0.0,
                    "ended_at": utc_now_iso(),
                    "updated_at": utc_now_iso(),
                },
            )
            await self._emit_run_update(run_id)
        await self._emit_run_log(run_id, "INFO", "Stop requested.")
        return {"ok": True, "run_id": run_id}

    async def approve_run(self, run_id: str) -> dict[str, Any]:
        run = await self._repository.get_run(run_id)
        if run is None:
            raise ValueError("Run not found.")
        if run["status"] != "COMPLETED":
            raise ValueError("Only COMPLETED runs can be approved.")
        metrics = dict(run.get("metrics", {}) or {})
        target_mode = str(((run.get("config", {}) or {}).get("target_mode", "triple_barrier") or "triple_barrier")).strip().lower()
        if target_mode == "trade_outcome":
            approval_ok = bool(metrics.get("approval_ok", False))
            walk_forward_ok = bool(metrics.get("walk_forward_approval_ok", False))
            deploy_allowed = bool(metrics.get("deploy_allowed", False))
            if not (approval_ok and walk_forward_ok and deploy_allowed):
                reason = str(metrics.get("approval_reason", "validation_overfit_or_test_failed") or "validation_overfit_or_test_failed")
                raise ValueError(f"Run cannot be approved for live use: {reason}.")
        pair_symbol = run["pair_symbol"]
        instance_id = str(run.get("instance_id", "") or "")
        instance = await self._repository.get_instance(instance_id) if instance_id else None
        instance_name = instance.name if instance else "Default"
        run_dir = self._run_dir(pair_symbol, run_id, instance_name=instance_name, instance_id=instance_id)
        model_path = run_dir / "model.pt"
        normalizer_path = run_dir / "normalizer_stats.json"
        if not model_path.exists() or not normalizer_path.exists():
            raise ValueError("Run artifacts missing (model/normalizer).")
        current_dir = self._instance_root(pair_symbol, instance_name, instance_id) / "current"
        current_dir.mkdir(parents=True, exist_ok=True)
        target_model = current_dir / "model.pt"
        target_norm = current_dir / "normalizer_stats.json"
        shutil.copyfile(model_path, target_model)
        shutil.copyfile(normalizer_path, target_norm)
        await self._repository.set_current_model(
            pair_symbol=pair_symbol,
            instance_id=instance_id,
            run_id=run_id,
            model_path=str(target_model),
            normalizer_path=str(target_norm),
            metrics=run.get("metrics", {}),
        )
        await self._emit_run_log(run_id, "INFO", "Run approved and promoted to current model.")
        await self._emit_run_update(run_id)
        return {"ok": True, "pair_symbol": pair_symbol, "run_id": run_id}

    async def run_status(self, run_id: str) -> dict[str, Any]:
        run = await self._repository.get_run(run_id)
        if run is None:
            raise ValueError("Run not found.")
        return run

    async def run_logs(self, run_id: str) -> list[dict[str, str]]:
        return await self._repository.run_logs(run_id)

    async def run_metrics(self, run_id: str) -> dict[str, Any]:
        run = await self._repository.get_run(run_id)
        if run is None:
            raise ValueError("Run not found.")
        epochs = await self._repository.list_run_epoch_metrics(run_id)
        prediction_trace = await self._repository.list_run_prediction_trace(run_id, limit=8000, epoch_index=0, split="test")
        prediction_trace_full = await self._repository.list_run_prediction_trace(run_id, limit=250000, epoch_index=0, split="test")
        captured_epochs_map: dict[str, list[int]] = {}
        for split in ("train", "val", "test"):
            captured_epochs = [item for item in await self._repository.list_run_prediction_trace_epochs(run_id, split=split) if int(item) > 0]
            captured_epochs_map[split] = captured_epochs
        cache_signature = json.dumps(captured_epochs_map, sort_keys=True)
        cached = self._epoch_summary_cache.get(run_id)
        epoch_summaries: list[dict[str, Any]] = []
        if cached and str(cached.get("signature", "")) == cache_signature:
            epoch_summaries = list(cached.get("rows", []))
        else:
            for split in ("train", "val", "test"):
                sample: list[dict[str, Any]] = []
                total = 0
                correct = 0
                entry_allowed = 0
                blocked = 0
                action_counts = {"LONG": 0, "SHORT": 0, "HOLD": 0}
                blocked_reasons: dict[str, int] = {}
                predicted_counts = {"up": 0, "flat": 0, "down": 0}
                up_total = 0
                down_total = 0
                abstain_total = 0
                abstain_correct = 0
                directional_predictions = 0
                directional_actions = 0
                long_when_up = 0
                short_when_down = 0
                false_long = 0
                false_short = 0
                for epoch_idx in captured_epochs_map.get(split, []):
                    sample = await self._repository.list_run_prediction_trace(
                        run_id,
                        limit=40000,
                        epoch_index=int(epoch_idx),
                        split=split,
                    )
                    if not sample:
                        continue
                    total = int(len(sample))
                    if total <= 0:
                        continue
                    correct = sum(1 for row in sample if bool(row.get("correct", False)))
                    entry_allowed = sum(1 for row in sample if bool(row.get("entry_allowed", False)))
                    blocked = total - entry_allowed
                    action_counts = {"LONG": 0, "SHORT": 0, "HOLD": 0}
                    blocked_reasons: dict[str, int] = {}
                    predicted_counts = {"up": 0, "flat": 0, "down": 0}
                    up_total = 0
                    down_total = 0
                    abstain_total = 0
                    abstain_correct = 0
                    directional_predictions = 0
                    directional_actions = 0
                    long_when_up = 0
                    short_when_down = 0
                    false_long = 0
                    false_short = 0
                    for row in sample:
                        action = str(row.get("action", "HOLD") or "HOLD").upper()
                        actual = str(row.get("actual_label", "flat") or "flat").lower()
                        if action not in action_counts:
                            action = "HOLD"
                        action_counts[action] = int(action_counts.get(action, 0)) + 1
                        if action in {"LONG", "SHORT"}:
                            directional_actions += 1
                        if action == "HOLD":
                            abstain_total += 1
                            if actual == "flat":
                                abstain_correct += 1
                        pred_lbl = str(row.get("predicted_label", "flat") or "flat").lower()
                        if pred_lbl not in {"up", "flat", "down"}:
                            pred_lbl = "flat"
                        predicted_counts[pred_lbl] = int(predicted_counts.get(pred_lbl, 0)) + 1
                        if pred_lbl in {"up", "down"}:
                            directional_predictions += 1
                        if actual == "up":
                            up_total += 1
                            if action == "LONG":
                                long_when_up += 1
                        if actual == "down":
                            down_total += 1
                            if action == "SHORT":
                                short_when_down += 1
                        if action == "LONG" and actual != "up":
                            false_long += 1
                        if action == "SHORT" and actual != "down":
                            false_short += 1
                        if bool(row.get("entry_allowed", False)):
                            continue
                        reason = str(row.get("blocked_reason", "") or "other_gate")
                        blocked_reasons[reason] = int(blocked_reasons.get(reason, 0)) + 1
                    action_total = int(action_counts.get("LONG", 0) + action_counts.get("SHORT", 0) + action_counts.get("HOLD", 0))
                    pred_total = int(predicted_counts.get("up", 0) + predicted_counts.get("flat", 0) + predicted_counts.get("down", 0))
                    await self._emit_run_log(
                        run_id,
                        "INFO",
                        (
                            f"Epoch summary integrity split={split} epoch={int(epoch_idx)} "
                            f"total={total} action_total={action_total} pred_total={pred_total}"
                        ),
                    )
                    if action_total != total or pred_total != total:
                        await self._emit_run_log(
                            run_id,
                            "WARNING",
                            (
                                f"Epoch summary mismatch split={split} epoch={int(epoch_idx)} "
                                f"total={total} action_total={action_total} pred_total={pred_total}"
                            ),
                        )
                if not sample or total <= 0:
                    continue
                bucket_edges = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 1.01]
                bucket_stats: list[dict[str, Any]] = []
                for lo, hi in zip(bucket_edges[:-1], bucket_edges[1:]):
                    rows_bucket = [
                        r for r in sample
                        if float(r.get("confidence", 0.0) or 0.0) >= lo and float(r.get("confidence", 0.0) or 0.0) < hi
                    ]
                    if not rows_bucket:
                        continue
                    rets = sorted(float(r.get("actual_future_return_pct", 0.0) or 0.0) for r in rows_bucket)
                    dir_rows = [
                        r for r in rows_bucket
                        if str(r.get("predicted_label", "flat")) in {"up", "down"} and str(r.get("actual_label", "flat")) in {"up", "down"}
                    ]
                    hits = sum(1 for r in dir_rows if str(r.get("predicted_label")) == str(r.get("actual_label")))
                    bucket_stats.append(
                        {
                            "bucket": f"{lo:.2f}-{min(hi, 1.0):.2f}",
                            "sample_count": int(len(rows_bucket)),
                            "avg_future_return_pct": float(sum(rets) / max(1, len(rets))),
                            "median_future_return_pct": float(rets[len(rets) // 2]),
                            "direction_hit_rate": float(hits / max(1, len(dir_rows))),
                            "low_confidence_bucket": bool(len(rows_bucket) < 200),
                        }
                    )
                # Confidence histogram/spread + label quality distribution for replay diagnostics.
                hist_bins: list[tuple[float, float, str]] = [
                    (0.33, 0.40, "0.33-0.40"),
                    (0.40, 0.50, "0.40-0.50"),
                    (0.50, 0.60, "0.50-0.60"),
                    (0.60, 1.01, "0.60+"),
                ]
                conf_hist: list[dict[str, Any]] = []
                for lo, hi, label in hist_bins:
                    rows_bin = [r for r in sample if float(r.get("confidence", 0.0) or 0.0) >= lo and float(r.get("confidence", 0.0) or 0.0) < hi]
                    conf_hist.append(
                        {
                            "bucket": label,
                            "count": int(len(rows_bin)),
                            "pct": float(len(rows_bin) / max(1, len(sample))),
                        }
                    )
                top1_arr = np.asarray([float(r.get("confidence", 0.0) or 0.0) for r in sample], dtype=np.float32)
                gap_arr = np.asarray([float(r.get("confidence_gap", 0.0) or 0.0) for r in sample], dtype=np.float32)
                top2_arr = np.maximum(0.0, top1_arr - gap_arr)
                spread_stats = {
                    "mean_top1_prob": float(np.mean(top1_arr)) if top1_arr.size else 0.0,
                    "mean_top2_prob": float(np.mean(top2_arr)) if top2_arr.size else 0.0,
                    "mean_confidence_gap": float(np.mean(gap_arr)) if gap_arr.size else 0.0,
                }
                label_rows: dict[str, list[float]] = {"up": [], "flat": [], "down": []}
                for r in sample:
                    lbl = str(r.get("actual_label", "flat") or "flat").lower()
                    if lbl in label_rows:
                        label_rows[lbl].append(float(r.get("actual_future_return_pct", 0.0) or 0.0))

                def _dist(values: list[float]) -> dict[str, Any]:
                    if not values:
                        return {"count": 0, "mean": 0.0, "median": 0.0, "p25": 0.0, "p75": 0.0, "min": 0.0, "max": 0.0}
                    arr = np.asarray(values, dtype=np.float64)
                    return {
                        "count": int(arr.size),
                        "mean": float(np.mean(arr)),
                        "median": float(np.median(arr)),
                        "p25": float(np.percentile(arr, 25)),
                        "p75": float(np.percentile(arr, 75)),
                        "min": float(np.min(arr)),
                        "max": float(np.max(arr)),
                    }

                label_dist = {
                    "up": _dist(label_rows["up"]),
                    "flat": _dist(label_rows["flat"]),
                    "down": _dist(label_rows["down"]),
                }
                epoch_summaries.append(
                    {
                        "epoch": int(epoch_idx),
                        "split": split,
                        "samples": int(total),
                        "accuracy": float(correct / max(1, total)),
                        "entry_allowed_rate": float(entry_allowed / max(1, total)),
                        "directional_prediction_rate": float(directional_predictions / max(1, total)),
                        "directional_action_rate": float(directional_actions / max(1, total)),
                        "abstention_rate": float(abstain_total / max(1, total)),
                        "abstention_correctness": float(abstain_correct / max(1, abstain_total)),
                        "blocked_count": int(blocked),
                        "action_counts": {
                            "LONG": int(action_counts.get("LONG", 0)),
                            "SHORT": int(action_counts.get("SHORT", 0)),
                            "HOLD": int(action_counts.get("HOLD", 0)),
                        },
                        "blocked_reasons": blocked_reasons or {},
                        "predicted_class_distribution": {
                            "up_count": int(predicted_counts.get("up", 0)),
                            "flat_count": int(predicted_counts.get("flat", 0)),
                            "down_count": int(predicted_counts.get("down", 0)),
                            "up_pct": float(predicted_counts.get("up", 0) / max(1, total)),
                            "flat_pct": float(predicted_counts.get("flat", 0) / max(1, total)),
                            "down_pct": float(predicted_counts.get("down", 0) / max(1, total)),
                            "total_rows": int(total),
                        },
                        "gate_paralyzed": bool(entry_allowed == 0 and (action_counts.get("LONG", 0) + action_counts.get("SHORT", 0)) == 0),
                        "trade_rate_by_class": {
                            "long_rate_when_actual_up": float(long_when_up / max(1, up_total)),
                            "short_rate_when_actual_down": float(short_when_down / max(1, down_total)),
                            "false_long_rate": float(false_long / max(1, action_counts.get("LONG", 0))),
                            "false_short_rate": float(false_short / max(1, action_counts.get("SHORT", 0))),
                            "actual_up_count": int(up_total),
                            "actual_down_count": int(down_total),
                            "pred_long_count": int(action_counts.get("LONG", 0)),
                            "pred_short_count": int(action_counts.get("SHORT", 0)),
                        },
                        "expected_edge_by_confidence_bucket": bucket_stats,
                        "confidence_distribution_histogram": conf_hist,
                        "confidence_spread_stats": spread_stats,
                        "future_return_distribution_by_label": label_dist,
                    }
                )
            self._epoch_summary_cache[run_id] = {"signature": cache_signature, "rows": list(epoch_summaries)}
        final_metrics = dict(run.get("metrics", {}) or {})
        expected_hash = str(final_metrics.get("decision_trace_hash", "") or "")
        replay_hash_payload = [
            {
                "ts_ms": int(r.get("ts_ms", 0) or 0),
                "predicted_label": str(r.get("predicted_label", "")),
                "actual_label": str(r.get("actual_label", "")),
                "action": str(r.get("action", "")),
                "entry_allowed": bool(r.get("entry_allowed", False)),
                "blocked_reason": str(r.get("blocked_reason", "")),
                "confidence": float(r.get("confidence", 0.0) or 0.0),
                "prob_gap": float(r.get("prob_gap", r.get("confidence_gap", 0.0)) or 0.0),
                "entropy": float(r.get("entropy", 0.0) or 0.0),
            }
            for r in prediction_trace_full
        ]
        actual_hash = hashlib.sha256(
            json.dumps(replay_hash_payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        ).hexdigest() if replay_hash_payload else ""
        final_metrics["decision_trace_hash_recomputed"] = actual_hash
        final_metrics["decision_trace_hash_match"] = bool(expected_hash and actual_hash and expected_hash == actual_hash)
        final_metrics["deterministic_replay_verified"] = bool(final_metrics["decision_trace_hash_match"])
        return {
            "run_id": run_id,
            "pair_symbol": run.get("pair_symbol", ""),
            "instance_id": run.get("instance_id", ""),
            "status": run.get("status", ""),
            "epochs": epochs,
            "captured_epochs": captured_epochs_map.get("test", []),
            "captured_epochs_by_split": captured_epochs_map,
            "epoch_summaries": epoch_summaries,
            "prediction_trace": prediction_trace,
            "final_metrics": final_metrics,
        }

    async def run_prediction_trace(
        self,
        run_id: str,
        *,
        split: str = "test",
        epoch_index: int = 0,
        limit: int = 250_000,
    ) -> dict[str, Any]:
        run = await self._repository.get_run(run_id)
        if run is None:
            raise ValueError("Run not found.")
        split_value = str(split or "test").strip().lower()
        if split_value not in {"train", "val", "test"}:
            split_value = "test"
        safe_limit = max(1, min(int(limit or 250_000), 500_000))
        rows = await self._repository.list_run_prediction_trace(
            run_id,
            limit=safe_limit,
            epoch_index=int(epoch_index),
            split=split_value,
        )
        return {
            "run_id": run_id,
            "split": split_value,
            "epoch_index": int(epoch_index),
            "limit": safe_limit,
            "rows": rows,
        }

    async def replay_start(
        self,
        *,
        run_id: str,
        source_mode: str = "prediction_trace",
        mode: str = "test_trace",
        speed: float = 1.0,
        epoch_index: int = 0,
        split: str = "test",
    ) -> dict[str, Any]:
        run = await self._repository.get_run(run_id)
        if run is None:
            raise ValueError("Run not found.")
        source_mode_value = str(source_mode or "prediction_trace").strip().lower()
        if source_mode_value not in {"prediction_trace", "market_event_replay"}:
            source_mode_value = "prediction_trace"
        split_value = str(split or "test").strip().lower()
        if split_value not in {"train", "val", "test"}:
            split_value = "test"
        frames: list[dict[str, Any]] = []
        parity_stats: dict[str, Any] = {}
        if source_mode_value == "market_event_replay":
            hint_frames = await self._repository.list_run_prediction_trace(
                run_id,
                limit=250_000,
                epoch_index=int(epoch_index),
                split=split_value,
            )
            ts_start_ms = min((int(item.get("ts_ms", 0) or 0) for item in hint_frames), default=0)
            ts_end_ms = max((int(item.get("ts_ms", 0) or 0) for item in hint_frames), default=0)
            frames, parity_stats = await self._build_market_event_replay_frames(
                pair_symbol=str(run.get("pair_symbol", "")).upper(),
                ts_start_ms=ts_start_ms if ts_start_ms > 0 else None,
                ts_end_ms=ts_end_ms if ts_end_ms > 0 else None,
            )
            if not frames:
                return {
                    "ok": False,
                    "available": False,
                    "reason": "market_event_replay_missing",
                    "run_id": run_id,
                    "split": split_value,
                    "epoch_index": int(epoch_index),
                    "regenerate_hint": "Enable full-fidelity recording and collect market events for this pair/time window.",
                }
        else:
            frames = await self._repository.list_run_prediction_trace(
                run_id,
                limit=250_000,
                epoch_index=int(epoch_index),
                split=split_value,
            )
            if not frames:
                if int(epoch_index) == 0:
                    return {
                        "ok": False,
                        "available": False,
                        "reason": "trace_not_captured_for_split",
                        "run_id": run_id,
                        "split": split_value,
                        "regenerate_hint": "Enable split capture in Pair Profile and retrain.",
                    }
                if int(epoch_index) > 0:
                    return {
                        "ok": False,
                        "available": False,
                        "reason": "trace_not_captured_for_epoch",
                        "run_id": run_id,
                        "epoch_index": int(epoch_index),
                        "split": split_value,
                        "regenerate_hint": "Add this epoch to epoch_trace_capture_list and retrain.",
                    }
                return {
                    "ok": False,
                    "available": False,
                    "reason": "replay_trace_missing",
                    "run_id": run_id,
                    "regenerate_hint": "Run training/evaluation again to generate replay trace.",
                }
            # Merge prediction trace with reconstructed market microstructure snapshots by nearest timestamp.
            ts_start_ms = min((int(item.get("ts_ms", 0) or 0) for item in frames), default=0)
            ts_end_ms = max((int(item.get("ts_ms", 0) or 0) for item in frames), default=0)
            market_frames, _market_stats = await self._build_market_event_replay_frames(
                pair_symbol=str(run.get("pair_symbol", "")).upper(),
                ts_start_ms=ts_start_ms if ts_start_ms > 0 else None,
                ts_end_ms=ts_end_ms if ts_end_ms > 0 else None,
            )
            if market_frames:
                market_by_ts = [
                    (int(mf.get("ts_ms", 0) or 0), (mf.get("terminal_snapshot") or {}))
                    for mf in market_frames
                    if int(mf.get("ts_ms", 0) or 0) > 0
                ]
                if market_by_ts:
                    market_by_ts.sort(key=lambda x: x[0])
                    merged: list[dict[str, Any]] = []
                    j = 0
                    n = len(market_by_ts)
                    for row in frames:
                        row_ts = int(row.get("ts_ms", 0) or 0)
                        if row_ts <= 0:
                            merged.append(dict(row))
                            continue
                        while j + 1 < n and market_by_ts[j + 1][0] <= row_ts:
                            j += 1
                        candidates = [market_by_ts[j]]
                        if j + 1 < n:
                            candidates.append(market_by_ts[j + 1])
                        best_ts, best_snap = min(candidates, key=lambda t: abs(t[0] - row_ts))
                        item = dict(row)
                        item["terminal_snapshot"] = dict(best_snap or {})
                        item["market_join_ts_ms"] = int(best_ts)
                        merged.append(item)
                    frames = merged
            parity_stats = {
                "source_mode": "prediction_trace",
                "expected_events": len(frames),
                "processed_events": 0,
                "batch_ms": 0,
                "frames_total": len(frames),
            }
        replay_id = f"replay_{int(time.time())}_{uuid4().hex[:8]}"
        now = utc_now_iso()
        session = _ReplaySession(
            replay_id=replay_id,
            run_id=run_id,
            pair_symbol=str(run.get("pair_symbol", "")).upper(),
            instance_id=str(run.get("instance_id", "")),
            source_mode=source_mode_value,
            mode=(mode or "test_trace"),
            status="paused",
            cursor=0,
            speed=max(0.1, float(speed or 1.0)),
            epoch_index=int(epoch_index),
            split=split_value,
            created_at=now,
            updated_at=now,
            frames=frames,
            parity_stats=parity_stats,
        )
        self._replay_sessions[replay_id] = session
        return {
            "ok": True,
            "replay_id": replay_id,
            "run_id": run_id,
            "pair_symbol": session.pair_symbol,
            "instance_id": session.instance_id,
            "status": session.status,
            "cursor": 0,
            "frames_total": len(frames),
            "source_mode": source_mode_value,
            "mode": session.mode,
            "speed": session.speed,
            "epoch_index": session.epoch_index,
            "split": session.split,
            "available": True,
        }

    async def replay_step(self, replay_id: str) -> dict[str, Any]:
        session = self._replay_sessions.get(replay_id)
        if session is None:
            raise ValueError("Replay session not found.")
        if not session.frames:
            return {
                "replay_id": replay_id,
                "status": session.status,
                "split": session.split,
                "frame": None,
                "cursor": 0,
                "frames_total": 0,
            }
        cursor = max(0, min(session.cursor, len(session.frames) - 1))
        frame = session.frames[cursor]
        if session.source_mode == "market_event_replay":
            session.updated_at = utc_now_iso()
            processed = int(frame.get("processed_events", cursor + 1) or (cursor + 1))
            session.parity_stats["processed_events"] = processed
            session.parity_stats["frames_processed"] = int(cursor + 1)
            return {
                "replay_id": replay_id,
                "status": session.status,
                "cursor": cursor,
                "frames_total": len(session.frames),
                "epoch_index": session.epoch_index,
                "split": session.split,
                "source_mode": session.source_mode,
                "frame": frame,
                "action_totals": {"LONG": 0, "SHORT": 0, "HOLD": 0},
                "action_seen": {"LONG": 0, "SHORT": 0, "HOLD": 0},
                "terminal_snapshot": frame.get("terminal_snapshot", {}),
                "unavailable_fields": [],
            }
        action_totals = {"LONG": 0, "SHORT": 0, "HOLD": 0}
        action_seen = {"LONG": 0, "SHORT": 0, "HOLD": 0}
        for idx, row in enumerate(session.frames):
            action = str((row or {}).get("action", "HOLD") or "HOLD").upper()
            if action not in action_totals:
                action = "HOLD"
            action_totals[action] = int(action_totals.get(action, 0)) + 1
            if idx <= cursor:
                action_seen[action] = int(action_seen.get(action, 0)) + 1
        terminal_snapshot = (frame.get("terminal_snapshot") if isinstance(frame.get("terminal_snapshot"), dict) else None) or {
            "type": "terminal_snapshot",
            "symbol": session.pair_symbol,
            "received_at": utc_now_iso(),
            "event_count": int(cursor + 1),
            "book_synced": False,
            "last_update_id": int(cursor),
            "best_bid": None,
            "best_ask": None,
            "spread": None,
            "mark_price": None,
            "funding_rate": None,
            "last_trade_price": None,
            "last_trade_qty": None,
            "last_trade_side": "Unavailable in replay source",
            "last_kline_close": None,
            "last_kline_interval": "1m",
            "last_liquidation_side": "Unavailable in replay source",
            "last_liquidation_qty": None,
            "trade_rate_10s": None,
            "liq_events_60s": None,
            "sync_failures": 0,
            "stream_counts": {},
            "depth_levels": {"bids": 0, "asks": 0},
            "decision_layer": {"replay_decision": frame, "status_note": "Unavailable in replay source: orderbook/heatmap/ladder/liquidations"},
        }
        if isinstance(terminal_snapshot, dict):
            dl = terminal_snapshot.get("decision_layer")
            if not isinstance(dl, dict):
                dl = {}
                terminal_snapshot["decision_layer"] = dl
            replay_decision = dict(frame) if isinstance(frame, dict) else {"value": frame}
            # Avoid circular reference: frame may include terminal_snapshot -> decision_layer.
            replay_decision.pop("terminal_snapshot", None)
            dl["replay_decision"] = replay_decision
        session.updated_at = utc_now_iso()
        unavailable = [] if frame.get("terminal_snapshot") else ["orderbook", "heatmap", "ladder", "liquidations"]
        return {
            "replay_id": replay_id,
            "status": session.status,
            "cursor": cursor,
            "frames_total": len(session.frames),
            "source_mode": session.source_mode,
            "epoch_index": session.epoch_index,
            "split": session.split,
            "frame": frame,
            "action_totals": action_totals,
            "action_seen": action_seen,
            "terminal_snapshot": terminal_snapshot,
            "unavailable_fields": unavailable,
        }

    async def replay_events(self, replay_id: str, since: int = 0, limit: int = 200) -> dict[str, Any]:
        session = self._replay_sessions.get(replay_id)
        if session is None:
            raise ValueError("Replay session not found.")
        start_idx = max(0, int(since))
        end_idx = min(len(session.frames), start_idx + max(1, int(limit)))
        events = []
        for idx in range(start_idx, end_idx):
            frame = session.frames[idx]
            events.append(
                {
                    "event_type": "ml_replay_step" if session.source_mode != "market_event_replay" else "market_event_replay_step",
                    "index": idx,
                    "ts_ms": int(frame.get("ts_ms", 0) or 0),
                    "frame": frame,
                }
            )
        return {
            "replay_id": replay_id,
            "status": session.status,
            "source_mode": session.source_mode,
            "epoch_index": session.epoch_index,
            "split": session.split,
            "since": start_idx,
            "next_since": end_idx,
            "events": events,
        }

    async def replay_control(
        self,
        replay_id: str,
        *,
        action: str = "pause",
        cursor: int | None = None,
        speed: float | None = None,
        source_mode: str | None = None,
        epoch_index: int | None = None,
        split: str | None = None,
    ) -> dict[str, Any]:
        session = self._replay_sessions.get(replay_id)
        if session is None:
            raise ValueError("Replay session not found.")
        requested_source_mode = session.source_mode
        if source_mode is not None:
            source_mode_value = str(source_mode or "").strip().lower()
            if source_mode_value in {"prediction_trace", "market_event_replay"}:
                requested_source_mode = source_mode_value
        if requested_source_mode != session.source_mode:
            if requested_source_mode == "market_event_replay":
                hint_frames = await self._repository.list_run_prediction_trace(
                    session.run_id,
                    limit=250_000,
                    epoch_index=int(epoch_index if epoch_index is not None else session.epoch_index),
                    split=str(split or session.split or "test").strip().lower(),
                )
                ts_start_ms = min((int(item.get("ts_ms", 0) or 0) for item in hint_frames), default=0)
                ts_end_ms = max((int(item.get("ts_ms", 0) or 0) for item in hint_frames), default=0)
                frames, stats = await self._build_market_event_replay_frames(
                    pair_symbol=session.pair_symbol,
                    ts_start_ms=ts_start_ms if ts_start_ms > 0 else None,
                    ts_end_ms=ts_end_ms if ts_end_ms > 0 else None,
                )
                if not frames:
                    return {
                        "ok": False,
                        "replay_id": replay_id,
                        "status": session.status,
                        "reason": "market_event_replay_missing",
                    }
                session.frames = frames
                session.parity_stats = stats
                session.source_mode = requested_source_mode
                session.cursor = 0
                session.status = "paused"
            else:
                next_split_init = str(split or session.split or "test").strip().lower()
                if next_split_init not in {"train", "val", "test"}:
                    next_split_init = "test"
                next_epoch_init = int(epoch_index if epoch_index is not None else session.epoch_index)
                frames = await self._repository.list_run_prediction_trace(
                    session.run_id,
                    limit=250_000,
                    epoch_index=next_epoch_init,
                    split=next_split_init,
                )
                if not frames:
                    return {
                        "ok": False,
                        "replay_id": replay_id,
                        "status": session.status,
                        "reason": "trace_not_captured_for_epoch" if next_epoch_init > 0 else "trace_not_captured_for_split",
                    }
                ts_start_ms = min((int(item.get("ts_ms", 0) or 0) for item in frames), default=0)
                ts_end_ms = max((int(item.get("ts_ms", 0) or 0) for item in frames), default=0)
                market_frames, _market_stats = await self._build_market_event_replay_frames(
                    pair_symbol=session.pair_symbol,
                    ts_start_ms=ts_start_ms if ts_start_ms > 0 else None,
                    ts_end_ms=ts_end_ms if ts_end_ms > 0 else None,
                )
                if market_frames:
                    market_by_ts = [
                        (int(mf.get("ts_ms", 0) or 0), (mf.get("terminal_snapshot") or {}))
                        for mf in market_frames
                        if int(mf.get("ts_ms", 0) or 0) > 0
                    ]
                    if market_by_ts:
                        market_by_ts.sort(key=lambda x: x[0])
                        merged: list[dict[str, Any]] = []
                        j = 0
                        n = len(market_by_ts)
                        for row in frames:
                            row_ts = int(row.get("ts_ms", 0) or 0)
                            if row_ts <= 0:
                                merged.append(dict(row))
                                continue
                            while j + 1 < n and market_by_ts[j + 1][0] <= row_ts:
                                j += 1
                            candidates = [market_by_ts[j]]
                            if j + 1 < n:
                                candidates.append(market_by_ts[j + 1])
                            best_ts, best_snap = min(candidates, key=lambda t: abs(t[0] - row_ts))
                            item = dict(row)
                            item["terminal_snapshot"] = dict(best_snap or {})
                            item["market_join_ts_ms"] = int(best_ts)
                            merged.append(item)
                        frames = merged
                session.frames = frames
                session.parity_stats = {
                    "source_mode": "prediction_trace",
                    "expected_events": len(frames),
                    "processed_events": 0,
                    "batch_ms": 0,
                    "frames_total": len(frames),
                }
                session.source_mode = requested_source_mode
                session.cursor = 0
                session.status = "paused"
        if session.source_mode == "market_event_replay":
            if speed is not None:
                session.speed = max(0.1, min(10.0, float(speed)))
            action_value = (action or "pause").strip().lower()
            if action_value in {"play", "resume"}:
                session.status = "playing"
            elif action_value in {"pause", "stop"}:
                session.status = "paused"
            elif action_value in {"close", "release"}:
                self._replay_sessions.pop(replay_id, None)
                return {"ok": True, "replay_id": replay_id, "closed": True}
            if cursor is not None:
                session.cursor = max(0, min(int(cursor), max(0, len(session.frames) - 1)))
            elif session.status == "playing" and session.frames:
                step_size = max(1, int(round(float(session.speed or 1.0))))
                session.cursor = max(0, min(session.cursor + step_size, len(session.frames) - 1))
                if session.cursor >= len(session.frames) - 1:
                    session.status = "paused"
            session.updated_at = utc_now_iso()
            return {
                "ok": True,
                "replay_id": replay_id,
                "status": session.status,
                "cursor": session.cursor,
                "frames_total": len(session.frames),
                "speed": session.speed,
                "source_mode": session.source_mode,
                "epoch_index": session.epoch_index,
                "split": session.split,
            }
        next_split = str(split or session.split or "test").strip().lower()
        if next_split not in {"train", "val", "test"}:
            next_split = "test"
        needs_reload = (epoch_index is not None and int(epoch_index) != int(session.epoch_index)) or (next_split != session.split)
        if needs_reload:
            next_epoch = int(epoch_index if epoch_index is not None else session.epoch_index)
            frames = await self._repository.list_run_prediction_trace(
                session.run_id,
                limit=250_000,
                epoch_index=next_epoch,
                split=next_split,
            )
            if not frames:
                return {
                    "ok": False,
                    "replay_id": replay_id,
                    "status": session.status,
                    "reason": "trace_not_captured_for_epoch" if next_epoch > 0 else "trace_not_captured_for_split",
                    "epoch_index": int(next_epoch),
                    "split": next_split,
                }
            session.epoch_index = int(next_epoch)
            session.split = next_split
            session.frames = frames
            session.cursor = 0
            session.status = "paused"
        if speed is not None:
            session.speed = max(0.1, min(10.0, float(speed)))
        action_value = (action or "pause").strip().lower()
        if action_value in {"play", "resume"}:
            session.status = "playing"
        elif action_value in {"pause", "stop"}:
            session.status = "paused"
        elif action_value in {"close", "release"}:
            self._replay_sessions.pop(replay_id, None)
            return {"ok": True, "replay_id": replay_id, "closed": True}
        if cursor is not None:
            session.cursor = max(0, min(int(cursor), max(0, len(session.frames) - 1)))
        elif session.status == "playing" and session.frames:
            step_size = max(1, int(round(float(session.speed or 1.0))))
            session.cursor = max(0, min(session.cursor + step_size, len(session.frames) - 1))
            if session.cursor >= len(session.frames) - 1:
                session.status = "paused"
        session.updated_at = utc_now_iso()
        return {
            "ok": True,
            "replay_id": replay_id,
            "status": session.status,
            "cursor": session.cursor,
            "frames_total": len(session.frames),
            "speed": session.speed,
            "source_mode": session.source_mode,
            "epoch_index": session.epoch_index,
            "split": session.split,
        }

    async def replay_parity_stats(self, replay_id: str) -> dict[str, Any]:
        session = self._replay_sessions.get(replay_id)
        if session is None:
            raise ValueError("Replay session not found.")
        stats = dict(session.parity_stats or {})
        stats["replay_id"] = replay_id
        stats["source_mode"] = session.source_mode
        stats["cursor"] = int(session.cursor)
        stats["frames_total"] = len(session.frames)
        return stats

    async def list_runs(self, pair_symbol: str | None = None, instance_id: str | None = None) -> list[dict[str, Any]]:
        if instance_id:
            instance = await self._repository.get_instance(instance_id)
            if instance is None:
                return []
            runs = await self._repository.list_runs(pair_symbol=instance.pair_symbol, limit=100)
            return [item for item in runs if str(item.get("instance_id", "") or "") in {"", instance_id}]
        return await self._repository.list_runs(
            pair_symbol=pair_symbol.upper() if pair_symbol else None,
            instance_id=instance_id,
            limit=100,
        )

    async def delete_run(self, run_id: str) -> dict[str, Any]:
        run = await self._repository.get_run(run_id)
        if run is None:
            raise ValueError("Run not found.")
        if self._active_run_id == run_id or str(run.get("status", "")).upper() == "RUNNING":
            raise ValueError("Cannot delete a running run.")

        control = self._controls.setdefault(run_id, _RunControl())
        control.stop = True
        control.pause = False
        self._queued_ids.discard(run_id)
        self._controls.pop(run_id, None)

        deleted = await self._repository.delete_run(run_id)
        if deleted is None:
            raise ValueError("Run not found.")

        pair_symbol = str(deleted.get("pair_symbol", "")).upper()
        instance_id = str(run.get("instance_id", "") or "")
        instance = await self._repository.get_instance(instance_id) if instance_id else None
        instance_name = instance.name if instance else "Default"
        run_dir = self._run_dir(pair_symbol, run_id, instance_name=instance_name, instance_id=instance_id)
        if run_dir.exists():
            shutil.rmtree(run_dir, ignore_errors=True)

        return {
            "ok": True,
            "run_id": run_id,
            "pair_symbol": pair_symbol,
        }

    async def start_backfill(self, pair_symbol: str) -> dict[str, Any]:
        default_instance = await self._default_instance(pair_symbol.upper())
        return await self.start_backfill_by_instance(default_instance["instance_id"])

    async def start_backfill_by_instance(self, instance_id: str) -> dict[str, Any]:
        instance = await self._repository.get_instance(instance_id)
        if instance is None:
            raise ValueError("Instance not found.")
        pair_symbol = instance.pair_symbol
        profile = await self.get_profile_by_instance(instance_id)
        if not bool(profile.get("use_historic_data", False)):
            raise ValueError("Enable 'Use Historic Data' in Pair Profile before acquiring.")
        selected_exchanges = self._training_exchanges(profile)
        if not selected_exchanges:
            raise ValueError("No training exchanges selected for this pair.")

        existing_job_id = self._instance_backfill_job.get(instance_id) or self._pair_backfill_job.get(pair_symbol)
        if existing_job_id and existing_job_id in self._backfill_jobs:
            existing_job = self._backfill_jobs[existing_job_id]
            if existing_job.status in {"RUNNING", "PAUSED"}:
                control = self._backfill_controls.setdefault(existing_job_id, _RunControl())
                control.pause = False
                existing_job.status = "RUNNING"
                existing_job.updated_at = utc_now_iso()
                await self._emit_backfill_update(existing_job)
                return {"ok": True, "job_id": existing_job_id, "resumed": True}

        job_id = f"backfill_{int(time.time())}_{uuid4().hex[:8]}"
        now = utc_now_iso()
        target_root = self._historic_data_root(pair_symbol, instance.name, instance_id)
        target_root.mkdir(parents=True, exist_ok=True)
        job = _BackfillJob(
            job_id=job_id,
            pair_symbol=pair_symbol,
            status="RUNNING",
            stage="resolve_symbols",
            progress=0.0,
            mode="exchange_api_hybrid",
            source_path="exchange_api_hybrid",
            target_path=str(target_root),
            days=max(1, int(profile.get("historic_data_days", 90))),
            selected_exchanges=sorted(selected_exchanges),
            exchange_progress=[],
            instance_id=instance_id,
            instance_name=instance.name,
            started_at=now,
            updated_at=now,
        )
        self._backfill_jobs[job_id] = job
        self._backfill_logs[job_id] = []
        self._backfill_controls[job_id] = _RunControl()
        self._pair_backfill_job[pair_symbol] = job_id
        self._instance_backfill_job[instance_id] = job_id
        task = asyncio.create_task(self._run_backfill_job_safe(job_id), name=f"ml-backfill-{pair_symbol.lower()}-{job_id[-6:]}")
        self._backfill_tasks[job_id] = task
        await self._emit_backfill_log(job_id, "INFO", f"Backfill job created for {pair_symbol}, window={job.days} days.")
        await self._emit_backfill_update(job)
        return {"ok": True, "job_id": job_id, "instance_id": instance_id, "resumed": False}

    async def pause_backfill(self, job_id: str) -> dict[str, Any]:
        job = self._backfill_jobs.get(job_id)
        if job is None:
            raise ValueError("Backfill job not found.")
        control = self._backfill_controls.setdefault(job_id, _RunControl())
        control.pause = True
        job.status = "PAUSED"
        job.updated_at = utc_now_iso()
        await self._emit_backfill_log(job_id, "INFO", "Pause requested.")
        await self._emit_backfill_update(job)
        return {"ok": True, "job_id": job_id}

    async def stop_backfill(self, job_id: str) -> dict[str, Any]:
        job = self._backfill_jobs.get(job_id)
        if job is None:
            raise ValueError("Backfill job not found.")
        control = self._backfill_controls.setdefault(job_id, _RunControl())
        control.stop = True
        control.pause = False
        if job.status not in {"COMPLETED", "FAILED", "STOPPED"}:
            job.status = "STOPPED"
            job.stage = "stopped"
            job.progress = 0.0
            job.ended_at = utc_now_iso()
            job.updated_at = utc_now_iso()
            await self._emit_backfill_log(job_id, "INFO", "Stop requested.")
            await self._emit_backfill_update(job)
        return {"ok": True, "job_id": job_id}

    async def clear_historic_data(self, pair_symbol: str) -> dict[str, Any]:
        default_instance = await self._default_instance(pair_symbol.upper())
        return await self.clear_historic_data_by_instance(default_instance["instance_id"])

    async def clear_historic_data_by_instance(self, instance_id: str) -> dict[str, Any]:
        instance = await self._repository.get_instance(instance_id)
        if instance is None:
            raise ValueError("Instance not found.")
        pair_symbol = instance.pair_symbol
        existing_job_id = self._instance_backfill_job.get(instance_id) or self._pair_backfill_job.get(pair_symbol)
        if existing_job_id and existing_job_id in self._backfill_jobs:
            job = self._backfill_jobs[existing_job_id]
            if job.status in {"RUNNING", "PAUSED", "QUEUED"}:
                raise ValueError("Cannot delete historic data while acquisition is running.")

        target_root = self._historic_data_root(pair_symbol, instance.name, instance_id)
        target_root.mkdir(parents=True, exist_ok=True)
        removed_files = 0
        removed_rows = 0

        manifest_path = target_root / "backfill_manifest.json"
        if manifest_path.exists():
            try:
                payload = json.loads(manifest_path.read_text(encoding="utf-8"))
                removed_rows = int(payload.get("rows", 0) or 0)
            except Exception:
                removed_rows = 0

        for date_dir in target_root.glob("date=*"):
            if not date_dir.is_dir():
                continue
            try:
                removed_files += len(list(date_dir.glob("*.parquet")))
            except Exception:
                pass
            shutil.rmtree(date_dir, ignore_errors=True)

        for legacy_file in target_root.glob("*.jsonl"):
            try:
                legacy_file.unlink(missing_ok=True)
                removed_files += 1
            except Exception:
                continue

        for meta_name in ("backfill_manifest.json", "coverage_report.json"):
            try:
                (target_root / meta_name).unlink(missing_ok=True)
            except Exception:
                continue

        self._instance_backfill_job.pop(instance_id, None)
        self._pair_backfill_job.pop(pair_symbol, None)

        return {
            "ok": True,
            "pair_symbol": pair_symbol,
            "instance_id": instance_id,
            "target_path": str(target_root),
            "removed_files": int(removed_files),
            "removed_rows": int(removed_rows),
        }

    async def backfill_status(self, job_id: str) -> dict[str, Any]:
        job = self._backfill_jobs.get(job_id)
        if job is None:
            raise ValueError("Backfill job not found.")
        logs = self._backfill_logs.get(job_id, [])
        return {
            **self._backfill_job_to_dict(job),
            "log_count": len(logs),
            "latest_log": logs[-1] if logs else None,
        }

    async def backfill_logs(self, job_id: str) -> list[dict[str, str]]:
        if job_id not in self._backfill_jobs:
            raise ValueError("Backfill job not found.")
        return list(self._backfill_logs.get(job_id, []))

    async def latest_backfill_for_pair(self, pair_symbol: str) -> dict[str, Any] | None:
        pair_symbol = pair_symbol.upper()
        job_id = self._pair_backfill_job.get(pair_symbol)
        if not job_id:
            job_id = self._restore_saved_backfill_job(pair_symbol)
            if not job_id:
                return None
        job = self._backfill_jobs.get(job_id)
        if job is None:
            job_id = self._restore_saved_backfill_job(pair_symbol)
            if not job_id:
                return None
            job = self._backfill_jobs.get(job_id)
            if job is None:
                return None
        return await self.backfill_status(job_id)

    async def latest_backfill_for_instance(self, instance_id: str) -> dict[str, Any] | None:
        job_id = self._instance_backfill_job.get(instance_id)
        if not job_id:
            return None
        if job_id not in self._backfill_jobs:
            return None
        return await self.backfill_status(job_id)

    async def _run_backfill_job_safe(self, job_id: str) -> None:
        try:
            await self._run_backfill_job(job_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            job = self._backfill_jobs.get(job_id)
            if job is not None and job.status not in {"COMPLETED", "STOPPED"}:
                job.status = "FAILED"
                job.stage = "failed"
                job.progress = 0.0
                job.error_text = str(exc)
                job.updated_at = utc_now_iso()
                job.ended_at = utc_now_iso()
                await self._emit_backfill_log(job_id, "ERROR", f"Backfill failed: {exc}")
                await self._emit_backfill_update(job)
        finally:
            self._backfill_tasks.pop(job_id, None)

    async def _run_backfill_job(self, job_id: str) -> None:
        job = self._backfill_jobs.get(job_id)
        if job is None:
            return
        control = self._backfill_controls.setdefault(job_id, _RunControl())
        pair_symbol = job.pair_symbol
        profile = await (self.get_profile_by_instance(job.instance_id) if job.instance_id else self.get_profile(pair_symbol))
        selected_exchanges = sorted(self._training_exchanges(profile))
        if not selected_exchanges:
            job.status = "FAILED"
            job.stage = "resolve_symbols"
            job.error_text = "No exchanges selected for this pair."
            job.updated_at = utc_now_iso()
            job.ended_at = utc_now_iso()
            await self._emit_backfill_log(job_id, "ERROR", job.error_text)
            await self._emit_backfill_update(job)
            return
        now_ms = int(time.time() * 1000)
        cutoff_ms = now_ms - (job.days * 24 * 60 * 60 * 1000)
        job.selected_exchanges = list(selected_exchanges)
        job.mode = "exchange_api_hybrid"
        job.source_path = "exchange_api_hybrid"
        job.exchange_progress = [
            {
                "exchange_id": exchange_id,
                "status": "pending",
                "rows": 0,
                "data_hours": 0.0,
                "reason": "pending",
                "features_available": {"ohlcv": False, "trades": False, "funding": False, "liquidations": False},
            }
            for exchange_id in selected_exchanges
        ]

        target_root = self._historic_data_root(pair_symbol, job.instance_name, job.instance_id)
        target_root.mkdir(parents=True, exist_ok=True)
        for date_dir in target_root.glob("date=*"):
            if date_dir.is_dir():
                shutil.rmtree(date_dir, ignore_errors=True)
        for meta_file in ("backfill_manifest.json", "coverage_report.json"):
            try:
                (target_root / meta_file).unlink(missing_ok=True)
            except Exception:
                pass

        job.stage = "resolve_symbols"
        job.progress = 0.03
        job.updated_at = utc_now_iso()
        await self._emit_backfill_log(job_id, "INFO", f"Resolved selection: exchanges={', '.join(selected_exchanges)}")
        await self._emit_backfill_update(job)

        def _sync_job_exchange(result: dict[str, Any]) -> None:
            for row in job.exchange_progress:
                if row.get("exchange_id") == result.get("exchange_id"):
                    row.update(result)
                    return
            job.exchange_progress.append(result)

        async def _control_hook() -> str:
            if control.stop:
                return "stop"
            while control.pause and not control.stop:
                if job.status != "PAUSED":
                    job.status = "PAUSED"
                    job.updated_at = utc_now_iso()
                    await self._emit_backfill_update(job)
                await asyncio.sleep(0.3)
            if control.stop:
                return "stop"
            if job.status == "PAUSED":
                job.status = "RUNNING"
                job.updated_at = utc_now_iso()
                await self._emit_backfill_update(job)
            return "run"

        all_rows: list[dict[str, Any]] = []
        job.stage = "fetch_ohlcv"
        job.progress = 0.08
        job.updated_at = utc_now_iso()
        await self._emit_backfill_update(job)

        total_exchanges = len(selected_exchanges)
        for index, exchange_id in enumerate(selected_exchanges, start=1):
            if control.stop:
                job.status = "STOPPED"
                job.stage = "stopped"
                job.progress = 0.0
                job.ended_at = utc_now_iso()
                job.updated_at = utc_now_iso()
                await self._emit_backfill_log(job_id, "INFO", "Backfill stopped by user.")
                await self._emit_backfill_update(job)
                return

            _sync_job_exchange(
                {
                    "exchange_id": exchange_id,
                    "status": "running",
                    "rows": 0,
                    "data_hours": 0.0,
                    "reason": "fetching",
                    "features_available": {"ohlcv": False, "trades": False, "funding": False, "liquidations": False},
                }
            )
            job.updated_at = utc_now_iso()
            await self._emit_backfill_update(job)
            await self._emit_backfill_log(job_id, "INFO", f"{exchange_id}: starting exchange-api historical fetch")

            output = await self._historic_fetcher.fetch_exchange_hybrid(
                exchange_id=exchange_id,
                pair_symbol=pair_symbol,
                since_ms=cutoff_ms,
                until_ms=now_ms,
                control_hook=_control_hook,
                log_hook=lambda message, _job_id=job_id: self._emit_backfill_log(_job_id, "INFO", message),
            )

            result_row = {
                "exchange_id": output.exchange_id,
                "exchange_name": output.exchange_name,
                "status": output.status,
                "rows": len(output.rows),
                "data_hours": round(float(output.data_hours), 4),
                "reason": output.reason,
                "features_available": dict(output.features_available),
                "resolved_symbol": output.resolved_symbol,
                "started_at_ms": output.started_at_ms,
                "ended_at_ms": output.ended_at_ms,
            }
            _sync_job_exchange(result_row)
            if output.rows:
                all_rows.extend(output.rows)
            await self._emit_backfill_log(
                job_id,
                "INFO",
                (
                    f"{exchange_id}: status={output.status} rows={len(output.rows)} "
                    f"hours={output.data_hours:.2f} reason={output.reason}"
                ),
            )
            step_progress = 0.10 + (index / max(1, total_exchanges)) * 0.62
            job.progress = min(0.74, step_progress)
            job.updated_at = utc_now_iso()
            await self._emit_backfill_update(job)

        if control.stop:
            job.status = "STOPPED"
            job.stage = "stopped"
            job.progress = 0.0
            job.ended_at = utc_now_iso()
            job.updated_at = utc_now_iso()
            await self._emit_backfill_log(job_id, "INFO", "Backfill stopped by user.")
            await self._emit_backfill_update(job)
            return

        job.stage = "fetch_optional_streams"
        job.progress = max(job.progress, 0.76)
        job.updated_at = utc_now_iso()
        await self._emit_backfill_update(job)

        if not all_rows:
            job.status = "FAILED"
            job.stage = "failed"
            job.progress = 0.0
            job.error_text = "No selected exchange yielded usable historical data."
            job.updated_at = utc_now_iso()
            job.ended_at = utc_now_iso()
            await self._emit_backfill_log(job_id, "ERROR", job.error_text)
            await self._write_backfill_metadata(
                pair_symbol=pair_symbol,
                source_path=job.source_path,
                target_root=target_root,
                days=job.days,
                selected_exchanges=selected_exchanges,
                mode=job.mode,
                exchange_progress=job.exchange_progress,
                files=[],
                total_rows=0,
                data_hours=0.0,
                min_ts_ms=None,
                max_ts_ms=None,
            )
            await self._emit_backfill_update(job)
            return

        job.stage = "build_features"
        job.progress = max(job.progress, 0.80)
        job.updated_at = utc_now_iso()
        await self._emit_backfill_update(job)

        all_rows.sort(key=lambda row: int(row.get("ts_ms", 0)))
        by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in all_rows:
            ts_ms = int(row.get("ts_ms", 0) or 0)
            if ts_ms <= 0:
                continue
            date_key = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            by_date[date_key].append(row)

        job.stage = "write_parquet"
        job.progress = max(job.progress, 0.85)
        job.updated_at = utc_now_iso()
        await self._emit_backfill_update(job)

        written_files = 0
        total_rows = 0
        min_ts: int | None = None
        max_ts: int | None = None
        manifest_rows: list[dict[str, Any]] = []
        date_items = sorted(by_date.items(), key=lambda item: item[0])
        for idx, (date_key, day_rows) in enumerate(date_items, start=1):
            if control.stop:
                job.status = "STOPPED"
                job.stage = "stopped"
                job.progress = 0.0
                job.ended_at = utc_now_iso()
                job.updated_at = utc_now_iso()
                await self._emit_backfill_log(job_id, "INFO", "Backfill stopped by user.")
                await self._emit_backfill_update(job)
                return
            date_dir = target_root / f"date={date_key}"
            date_dir.mkdir(parents=True, exist_ok=True)
            file_name = f"part_{int(time.time() * 1000)}_{uuid4().hex[:8]}.parquet"
            dst_file = date_dir / file_name
            actual_file = await self._write_feature_records(dst_file, day_rows)
            ts_values = [int(item.get("ts_ms", 0)) for item in day_rows if int(item.get("ts_ms", 0)) > 0]
            if not ts_values:
                continue
            day_min = min(ts_values)
            day_max = max(ts_values)
            min_ts = day_min if min_ts is None else min(min_ts, day_min)
            max_ts = day_max if max_ts is None else max(max_ts, day_max)
            total_rows += len(day_rows)
            written_files += 1
            source_exchanges = sorted({str(item.get("source_exchange_id", "")).lower() for item in day_rows if str(item.get("source_exchange_id", "")).strip()})
            manifest_rows.append(
                {
                    "file_path": str(actual_file),
                    "date_key": date_key,
                    "row_count": len(day_rows),
                    "ts_start_ms": day_min,
                    "ts_end_ms": day_max,
                    "source_exchanges": source_exchanges,
                }
            )
            write_progress = 0.86 + (idx / max(1, len(date_items))) * 0.10
            job.progress = min(0.96, write_progress)
            job.updated_at = utc_now_iso()
            await self._emit_backfill_update(job)

        job.stage = "finalize"
        job.progress = 0.9
        job.updated_at = utc_now_iso()
        await self._emit_backfill_update(job)

        data_hours = 0.0
        if min_ts is not None and max_ts is not None and max_ts > min_ts:
            data_hours = (max_ts - min_ts) / 3_600_000
        await self._write_backfill_metadata(
            pair_symbol=pair_symbol,
            source_path=job.source_path,
            target_root=target_root,
            days=job.days,
            selected_exchanges=selected_exchanges,
            mode=job.mode,
            exchange_progress=job.exchange_progress,
            files=manifest_rows,
            total_rows=total_rows,
            data_hours=data_hours,
            min_ts_ms=min_ts,
            max_ts_ms=max_ts,
        )
        job.status = "COMPLETED"
        job.stage = "completed"
        job.progress = 1.0
        job.updated_at = utc_now_iso()
        job.ended_at = utc_now_iso()
        await self._emit_backfill_log(
            job_id,
            "INFO",
            f"Backfill completed. files={written_files} rows={total_rows} data_hours={data_hours:.2f}",
        )
        await self._emit_backfill_update(job)

    def _source_feature_files(self, pair_symbol: str, cutoff_ms: int) -> list[Path]:
        source_root = self._feature_data_root / pair_symbol.upper()
        if not source_root.exists():
            return []
        cutoff_date = datetime.fromtimestamp(cutoff_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        files: list[Path] = []
        for date_dir in sorted(source_root.glob("date=*")):
            if not date_dir.is_dir():
                continue
            date_key = date_dir.name.replace("date=", "", 1)
            if date_key and date_key < cutoff_date:
                continue
            for file_path in sorted(date_dir.iterdir()):
                if file_path.suffix.lower() not in {".parquet", ".jsonl"}:
                    continue
                files.append(file_path)
        return files

    async def _write_feature_records(self, file_path: Path, rows: list[dict[str, Any]]) -> Path:
        if pq is not None and file_path.suffix.lower() == ".parquet":
            import pyarrow as pa

            table = pa.Table.from_pylist(rows)
            pq.write_table(table, file_path, compression="zstd")
            return file_path
        if file_path.suffix.lower() == ".parquet":
            file_path = file_path.with_suffix(".jsonl")
        with file_path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=True) + "\n")
        return file_path

    async def _write_backfill_metadata(
        self,
        *,
        pair_symbol: str,
        source_path: str,
        target_root: Path,
        days: int,
        selected_exchanges: list[str],
        mode: str,
        exchange_progress: list[dict[str, Any]],
        files: list[dict[str, Any]],
        total_rows: int,
        data_hours: float,
        min_ts_ms: int | None,
        max_ts_ms: int | None,
    ) -> None:
        now = utc_now_iso()
        manifest_payload = {
            "pair_symbol": pair_symbol,
            "source_path": source_path,
            "target_path": str(target_root),
            "mode": mode,
            "days": days,
            "selected_exchanges": selected_exchanges,
            "generated_at": now,
            "file_count": len(files),
            "total_rows": total_rows,
            "exchange_progress": exchange_progress,
            "files": files,
        }
        coverage_payload = {
            "pair_symbol": pair_symbol,
            "source_path": source_path,
            "target_path": str(target_root),
            "mode": mode,
            "days": days,
            "selected_exchanges": selected_exchanges,
            "generated_at": now,
            "file_count": len(files),
            "total_rows": total_rows,
            "data_hours": data_hours,
            "min_ts_ms": min_ts_ms,
            "max_ts_ms": max_ts_ms,
            "exchange_progress": exchange_progress,
        }
        (target_root / "backfill_manifest.json").write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
        (target_root / "coverage_report.json").write_text(json.dumps(coverage_payload, indent=2), encoding="utf-8")

    def _backfill_job_to_dict(self, job: _BackfillJob) -> dict[str, Any]:
        return {
            "job_id": job.job_id,
            "pair_symbol": job.pair_symbol,
            "status": job.status,
            "stage": job.stage,
            "progress": job.progress,
            "mode": job.mode,
            "source_path": job.source_path,
            "target_path": job.target_path,
            "days": job.days,
            "instance_id": job.instance_id,
            "instance_name": job.instance_name,
            "selected_exchanges": list(job.selected_exchanges),
            "exchange_progress": list(job.exchange_progress),
            "started_at": job.started_at,
            "updated_at": job.updated_at,
            "ended_at": job.ended_at,
            "error_text": job.error_text,
        }

    def _restore_saved_backfill_job(self, pair_symbol: str) -> str | None:
        root = self._historic_data_root(pair_symbol)
        report_path = root / "coverage_report.json"
        if not report_path.exists():
            return None
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except Exception:
            return None
        generated_at = str(report.get("generated_at") or utc_now_iso())
        days = int(report.get("days", 90) or 90)
        selected_exchanges = [str(item).lower() for item in report.get("selected_exchanges", []) if str(item).strip()]
        exchange_progress = [item for item in report.get("exchange_progress", []) if isinstance(item, dict)]
        mode = str(report.get("mode") or "exchange_api_hybrid")
        job_id = f"backfill_saved_{pair_symbol.lower()}"
        if job_id in self._backfill_jobs:
            self._pair_backfill_job[pair_symbol.upper()] = job_id
            return job_id
        job = _BackfillJob(
            job_id=job_id,
            pair_symbol=pair_symbol.upper(),
            status="COMPLETED",
            stage="completed",
            progress=1.0,
            mode=mode,
            source_path=str(report.get("source_path") or (self._feature_data_root / pair_symbol.upper())),
            target_path=str(report.get("target_path") or root),
            days=max(1, days),
            selected_exchanges=selected_exchanges,
            exchange_progress=exchange_progress,
            started_at=generated_at,
            updated_at=generated_at,
            ended_at=generated_at,
            error_text="",
        )
        self._backfill_jobs[job_id] = job
        self._backfill_logs.setdefault(job_id, [])
        self._backfill_controls.setdefault(job_id, _RunControl())
        self._pair_backfill_job[pair_symbol.upper()] = job_id
        return job_id

    async def _emit_backfill_update(self, job: _BackfillJob) -> None:
        await self._hub.broadcast({"type": "ml_backfill_update", "job": self._backfill_job_to_dict(job)})

    async def _emit_backfill_log(self, job_id: str, level: str, message: str) -> None:
        now = utc_now_iso()
        logs = self._backfill_logs.setdefault(job_id, [])
        logs.append({"ts": now, "level": level.upper(), "message": message})
        if len(logs) > 1000:
            del logs[: len(logs) - 1000]
        await self._hub.broadcast(
            {
                "type": "ml_backfill_log",
                "job_id": job_id,
                "ts": now,
                "level": level.upper(),
                "message": message,
            }
        )

    async def start_bot(self, pair_symbol: str) -> dict[str, Any]:
        default_instance = await self._default_instance(pair_symbol.upper())
        return await self.start_bot_by_instance(default_instance["instance_id"])

    async def start_bot_by_instance(self, instance_id: str) -> dict[str, Any]:
        instance = await self._repository.get_instance(instance_id)
        if instance is None:
            raise ValueError("Instance not found.")
        profile = await self.get_profile_by_instance(instance_id)
        if not self._bot_data_exchanges(profile):
            raise ValueError("No bot data exchanges selected.")
        if not self._bot_execution_exchanges(profile):
            raise ValueError("No bot execution exchanges selected.")
        await self._repository.default_session(instance_id)
        pair_symbol = instance.pair_symbol
        if instance_id in self._bot_tasks and not self._bot_tasks[instance_id].done():
            self._bot_pause_flags[instance_id] = False
            state = await self._repository.bot_state(pair_symbol, instance_id=instance_id) or {}
            state["status"] = "RUNNING"
            await self._repository.upsert_bot_state(pair_symbol, state, instance_id=instance_id)
            await self._emit_bot_update(pair_symbol, instance_id=instance_id)
            return {"ok": True, "pair_symbol": pair_symbol, "instance_id": instance_id, "status": "RUNNING"}
        stop_event = asyncio.Event()
        self._bot_stop_flags[instance_id] = stop_event
        self._bot_pause_flags[instance_id] = False
        task = asyncio.create_task(
            self._bot_loop(pair_symbol, stop_event, instance_id=instance_id),
            name=f"ml-paper-bot-{pair_symbol.lower()}-{instance_id[-6:]}",
        )
        self._bot_tasks[instance_id] = task
        return {"ok": True, "pair_symbol": pair_symbol, "instance_id": instance_id, "status": "RUNNING"}

    async def pause_bot(self, pair_symbol: str) -> dict[str, Any]:
        default_instance = await self._default_instance(pair_symbol.upper())
        return await self.pause_bot_by_instance(default_instance["instance_id"])

    async def pause_bot_by_instance(self, instance_id: str) -> dict[str, Any]:
        instance = await self._repository.get_instance(instance_id)
        if instance is None:
            raise ValueError("Instance not found.")
        pair_symbol = instance.pair_symbol
        task = self._bot_tasks.get(instance_id)
        if task is None or task.done():
            return {"ok": True, "pair_symbol": pair_symbol, "instance_id": instance_id, "status": "IDLE"}
        self._bot_pause_flags[instance_id] = True
        state = await self._repository.bot_state(pair_symbol, instance_id=instance_id) or {}
        state["status"] = "PAUSED"
        last_signal = state.get("last_signal") if isinstance(state.get("last_signal"), dict) else {}
        if not isinstance(last_signal, dict):
            last_signal = {}
        if not last_signal:
            last_signal = {"action": "HOLD"}
        last_signal["reason"] = "Paused by user"
        state["last_signal"] = last_signal
        await self._repository.upsert_bot_state(pair_symbol, state, instance_id=instance_id)
        await self._emit_bot_update(pair_symbol, instance_id=instance_id)
        return {"ok": True, "pair_symbol": pair_symbol, "instance_id": instance_id, "status": "PAUSED"}

    async def stop_bot(self, pair_symbol: str) -> dict[str, Any]:
        default_instance = await self._default_instance(pair_symbol.upper())
        return await self.stop_bot_by_instance(default_instance["instance_id"])

    async def stop_bot_by_instance(self, instance_id: str) -> dict[str, Any]:
        instance = await self._repository.get_instance(instance_id)
        if instance is None:
            raise ValueError("Instance not found.")
        pair_symbol = instance.pair_symbol
        stop_event = self._bot_stop_flags.get(instance_id)
        if stop_event is not None:
            stop_event.set()
        task = self._bot_tasks.get(instance_id)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._bot_tasks.pop(instance_id, None)
        self._bot_stop_flags.pop(instance_id, None)
        self._bot_pause_flags.pop(instance_id, None)
        await self._repository.upsert_bot_state(
            pair_symbol,
            {
                "status": "IDLE",
                "position_side": "",
                "entry_price": 0.0,
                "qty": 0.0,
                "unrealized_pnl": 0.0,
                "realized_pnl": 0.0,
                "last_signal": {},
            },
            instance_id=instance_id,
        )
        await self._emit_bot_update(pair_symbol, instance_id=instance_id)
        return {"ok": True, "pair_symbol": pair_symbol, "instance_id": instance_id, "status": "IDLE"}

    async def bot_status(self, pair_symbol: str) -> dict[str, Any]:
        default_instance = await self._default_instance(pair_symbol.upper())
        return await self.bot_status_by_instance(default_instance["instance_id"])

    async def bot_status_by_instance(self, instance_id: str) -> dict[str, Any]:
        instance = await self._repository.get_instance(instance_id)
        if instance is None:
            raise ValueError("Instance not found.")
        pair_symbol = instance.pair_symbol
        profile = await self._repository.get_instance_profile(instance_id, pair_symbol)
        initial_balance = max(1.0, float(profile.paper_bot.get("initial_balance", 100.0)))
        default_session = await self._repository.default_session(instance_id)
        active_session_id = await self._repository.active_session_id(instance_id)
        if not active_session_id:
            active_session_id = default_session.session_id
            await self._repository.set_active_session(instance_id, active_session_id)
        state = await self._repository.bot_state(pair_symbol, instance_id=instance_id)
        if state is None:
            return {
                "pair_symbol": pair_symbol,
                "instance_id": instance_id,
                "status": "IDLE",
                "position_side": "",
                "entry_price": 0.0,
                "qty": 0.0,
                "unrealized_pnl": 0.0,
                "realized_pnl": 0.0,
                "initial_balance": initial_balance,
                "current_balance": initial_balance,
                "equity": initial_balance,
                "bot_data_source": "",
                "snapshot_freshness_sec": None,
                "active_exchange_count": 0,
                "bot_signal_mode": str(profile.bot_signal_mode or "combined"),
                "execution_position_mode": str(profile.execution_position_mode or "combined_position"),
                "training_selected_exchanges": list(profile.training_selected_exchanges or profile.selected_exchanges),
                "bot_data_selected_exchanges": list(profile.bot_data_selected_exchanges or profile.selected_exchanges),
                "bot_execution_selected_exchanges": list(profile.bot_execution_selected_exchanges or profile.selected_exchanges),
                "last_signal": {},
                "active_session_id": active_session_id,
                "updated_at": utc_now_iso(),
            }
        state["instance_id"] = instance_id
        state["active_session_id"] = str(state.get("active_session_id") or active_session_id)
        realized_pnl = float(state.get("realized_pnl") or 0.0)
        unrealized_pnl = float(state.get("unrealized_pnl") or 0.0)
        current_balance = initial_balance + realized_pnl
        equity = current_balance + unrealized_pnl
        state["initial_balance"] = initial_balance
        state["current_balance"] = current_balance
        state["equity"] = equity
        last_signal = state.get("last_signal")
        if not isinstance(last_signal, dict):
            last_signal = {}
        last_signal["initial_balance"] = round(initial_balance, 6)
        last_signal["current_balance"] = round(current_balance, 6)
        last_signal["equity"] = round(equity, 6)
        state["last_signal"] = last_signal
        state["bot_data_source"] = str(last_signal.get("data_source", "") or "")
        state["bot_signal_mode"] = str(last_signal.get("bot_signal_mode") or profile.bot_signal_mode or "combined")
        state["execution_position_mode"] = str(
            last_signal.get("execution_position_mode") or profile.execution_position_mode or "combined_position"
        )
        state["training_selected_exchanges"] = list(profile.training_selected_exchanges or profile.selected_exchanges)
        state["bot_data_selected_exchanges"] = list(profile.bot_data_selected_exchanges or profile.selected_exchanges)
        state["bot_execution_selected_exchanges"] = list(
            profile.bot_execution_selected_exchanges or profile.selected_exchanges
        )
        freshness = last_signal.get("snapshot_freshness_sec")
        try:
            state["snapshot_freshness_sec"] = float(freshness) if freshness is not None else None
        except Exception:
            state["snapshot_freshness_sec"] = None
        try:
            state["active_exchange_count"] = int(last_signal.get("active_exchange_count", 0) or 0)
        except Exception:
            state["active_exchange_count"] = 0
        return state

    async def bot_trades(
        self,
        pair_symbol: str,
        limit: int = 200,
        session_id: str = "",
        session_scope: str = "selected",
    ) -> dict[str, Any]:
        default_instance = await self._default_instance(pair_symbol.upper())
        return await self.bot_trades_by_instance(
            default_instance["instance_id"],
            limit=limit,
            session_id=session_id,
            session_scope=session_scope,
        )

    async def bot_trades_by_instance(
        self,
        instance_id: str,
        limit: int = 200,
        session_id: str = "",
        session_scope: str = "selected",
    ) -> dict[str, Any]:
        instance = await self._repository.get_instance(instance_id)
        if instance is None:
            raise ValueError("Instance not found.")
        pair_symbol = instance.pair_symbol
        await self._repository.default_session(instance_id)
        active_session_id = await self._repository.active_session_id(instance_id)
        selected_session_id = session_id.strip() if session_id else active_session_id
        scope = (session_scope or "selected").strip().lower()
        if scope not in {"selected", "all"}:
            scope = "selected"
        query_session_id = "" if scope == "all" else selected_session_id
        trades = await self._repository.list_bot_trades(
            pair_symbol,
            instance_id=instance_id,
            limit=limit,
            session_id=query_session_id,
        )
        return {
            "instance_id": instance_id,
            "pair_symbol": pair_symbol,
            "active_session_id": active_session_id,
            "session_id": selected_session_id,
            "session_scope": scope,
            "count": len(trades),
            "trades": trades,
        }

    async def bot_sessions_by_instance(self, instance_id: str) -> dict[str, Any]:
        instance = await self._repository.get_instance(instance_id)
        if instance is None:
            raise ValueError("Instance not found.")
        default_session = await self._repository.default_session(instance_id)
        active_session_id = await self._repository.active_session_id(instance_id)
        if not active_session_id:
            active_session_id = default_session.session_id
            await self._repository.set_active_session(instance_id, active_session_id)
        sessions = await self._repository.list_bot_trade_sessions(instance_id, include_archived=False)
        return {
            "instance_id": instance_id,
            "pair_symbol": instance.pair_symbol,
            "active_session_id": active_session_id,
            "sessions": [
                {
                    "session_id": s.session_id,
                    "instance_id": s.instance_id,
                    "name": s.name,
                    "is_default": s.is_default,
                    "created_at": s.created_at,
                    "updated_at": s.updated_at,
                    "archived": s.archived,
                    "trade_count": s.trade_count,
                }
                for s in sessions
            ],
        }

    async def create_bot_session_by_instance(self, instance_id: str, name: str) -> dict[str, Any]:
        instance = await self._repository.get_instance(instance_id)
        if instance is None:
            raise ValueError("Instance not found.")
        clean_name = " ".join(str(name or "").strip().split())
        if not clean_name:
            raise ValueError("Session name is required.")
        if len(clean_name) > 80:
            raise ValueError("Session name is too long (max 80 chars).")
        await self._repository.create_bot_trade_session(instance_id, clean_name, is_default=False)
        return await self.bot_sessions_by_instance(instance_id)

    async def update_bot_session_by_instance(
        self,
        instance_id: str,
        session_id: str,
        *,
        name: str | None = None,
        set_active: bool | None = None,
    ) -> dict[str, Any]:
        instance = await self._repository.get_instance(instance_id)
        if instance is None:
            raise ValueError("Instance not found.")
        if name is not None:
            clean_name = " ".join(str(name or "").strip().split())
            if not clean_name:
                raise ValueError("Session name is required.")
            if len(clean_name) > 80:
                raise ValueError("Session name is too long (max 80 chars).")
            await self._repository.rename_bot_trade_session(instance_id, session_id, clean_name)
        if set_active is True:
            await self._repository.set_active_session(instance_id, session_id)
        return await self.bot_sessions_by_instance(instance_id)

    async def delete_bot_session_by_instance(self, instance_id: str, session_id: str) -> dict[str, Any]:
        instance = await self._repository.get_instance(instance_id)
        if instance is None:
            raise ValueError("Instance not found.")
        default_session = await self._repository.default_session(instance_id)
        await self._repository.delete_bot_trade_session(instance_id, session_id, default_session.session_id)
        return await self.bot_sessions_by_instance(instance_id)

    async def clear_bot_session_history_by_instance(self, instance_id: str, session_id: str) -> dict[str, Any]:
        instance = await self._repository.get_instance(instance_id)
        if instance is None:
            raise ValueError("Instance not found.")
        deleted = await self._repository.clear_bot_trade_session_history(instance_id, session_id)
        payload = await self.bot_sessions_by_instance(instance_id)
        payload["deleted_trades"] = deleted
        return payload

    async def _enqueue_run(self, run_id: str) -> None:
        if run_id in self._queued_ids:
            return
        self._queued_ids.add(run_id)
        await self._queue.put(run_id)

    async def _consume_loop(self) -> None:
        while not self._stop.is_set():
            run_id = await self._queue.get()
            self._queued_ids.discard(run_id)
            control = self._controls.setdefault(run_id, _RunControl())
            if control.stop:
                continue
            while self._active_run_id is not None and not self._stop.is_set():
                await asyncio.sleep(0.3)
            # Stop may be requested while this run is waiting in queue.
            if control.stop:
                continue
            if self._stop.is_set():
                break
            self._active_run_id = run_id
            try:
                await self._execute_run(run_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("ML run execution error for %s: %s", run_id, exc)
                await self._repository.update_run(
                    run_id,
                    {
                        "status": "FAILED",
                        "stage": "failed",
                        "stage_progress": 0.0,
                        "ended_at": utc_now_iso(),
                        "updated_at": utc_now_iso(),
                        "error_text": str(exc),
                    },
                )
                await self._emit_run_log(run_id, "ERROR", f"Run failed: {exc}")
                await self._emit_run_update(run_id)
            finally:
                self._active_run_id = None

    async def _execute_run(self, run_id: str) -> None:
        run = await self._repository.get_run(run_id)
        if run is None:
            return
        pair_symbol = run["pair_symbol"]
        instance_id = str(run.get("instance_id", "") or "")
        instance = await self._repository.get_instance(instance_id) if instance_id else None
        instance_name = instance.name if instance else "Default"
        control = self._controls.setdefault(run_id, _RunControl())
        await self._repository.update_run(
            run_id,
            {
                "status": "RUNNING",
                "started_at": run.get("started_at") or utc_now_iso(),
                "updated_at": utc_now_iso(),
                "error_text": "",
            },
        )
        await self._emit_run_update(run_id)
        await self._emit_run_log(run_id, "INFO", f"Starting run for {pair_symbol}")
        await self._emit_run_log(
            run_id,
            "INFO",
            "Signal-quality diagnostics enabled: ev=true margin_sweep=true top_k=true confidence_buckets=true baselines=true",
        )
        if instance_id:
            profile = await self._repository.get_instance_profile(instance_id, pair_symbol)
        else:
            profile = await self._repository.get_profile(pair_symbol)
        config = self._profile_to_dict(profile)
        run_config = run.get("config", {})
        if isinstance(run_config, dict):
            dataset_source = str(run_config.get("dataset_source", "") or "").strip().lower()
            if dataset_source in {"features_manifest", "local_market_events"}:
                config["dataset_source"] = dataset_source
            config["training_ram_budget_gb"] = self._normalize_training_ram_budget_gb(
                run_config.get("training_ram_budget_gb")
            )
            config["training_prefetch_enabled"] = bool(run_config.get("training_prefetch_enabled", False))
            if "training_chunk_group_size" in run_config:
                config["training_chunk_group_size"] = run_config.get("training_chunk_group_size")
            if "training_process_recycle_enabled" in run_config:
                config["training_process_recycle_enabled"] = bool(run_config.get("training_process_recycle_enabled"))
            if "training_worker_groups_before_restart" in run_config:
                try:
                    config["training_worker_groups_before_restart"] = int(max(1, int(run_config.get("training_worker_groups_before_restart") or 1)))
                except Exception:
                    config["training_worker_groups_before_restart"] = 1
            if "training_worker_memory_cap_gb" in run_config:
                config["training_worker_memory_cap_gb"] = self._normalize_training_ram_budget_gb(run_config.get("training_worker_memory_cap_gb"))
            if "keep_training_shards" in run_config:
                config["keep_training_shards"] = bool(run_config.get("keep_training_shards"))
        if not self._training_exchanges(config):
            defaults = await self._default_selected_exchanges(pair_symbol)
            config["selected_exchanges"] = list(defaults)
            config["training_selected_exchanges"] = list(defaults)
            config["bot_data_selected_exchanges"] = list(defaults)
            config["bot_execution_selected_exchanges"] = list(defaults)
            if instance_id:
                await self.save_profile_by_instance(instance_id, config)
            else:
                await self.save_profile(pair_symbol, config)
        dataset_source_mode = str(config.get("dataset_source", "features_manifest") or "features_manifest").strip().lower()
        if dataset_source_mode not in {"features_manifest", "local_market_events"}:
            dataset_source_mode = "features_manifest"
            config["dataset_source"] = dataset_source_mode
        if dataset_source_mode == "features_manifest" and not bool(config.get("use_local_data", True)) and not bool(config.get("use_historic_data", False)):
            await self._mark_failed(run_id, "Both local and historic data sources are disabled in Pair Profile.")
            return
        if dataset_source_mode == "features_manifest" and not self._training_exchanges(config):
            await self._mark_failed(run_id, "No training exchanges selected in Pair Profile.")
            return
        await self._write_pair_settings_file(pair_symbol, config, instance_name=instance_name, instance_id=instance_id)
        run_dir = self._run_dir(pair_symbol, run_id, instance_name=instance_name, instance_id=instance_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        # Best-effort boundary cleanup before starting a new run. This reduces carry-over
        # RSS from prior runs in long-lived backend processes.
        await self._runtime_boundary_cleanup(run_id=run_id, reason="run_start_prewarm_cleanup", force=True)
        rss_at_start = self._process_tree_resident_memory_bytes() or 0
        await self._emit_run_log(
            run_id,
            "INFO",
            f"Parent RSS at run start={rss_at_start / float(1024**3):.3f}GB",
        )

        await self._set_stage(run_id, "data", 0.05, "Validating dataset availability")
        await self._wait_if_paused(run_id)
        if control.stop:
            await self._mark_stopped(run_id, "Stopped before data stage.")
            return
        if dataset_source_mode == "local_market_events":
            data_status = await self._market_events_data_status(pair_symbol)
        elif instance_id:
            data_status = await self.data_status_by_instance(instance_id)
        else:
            data_status = await self.data_status(pair_symbol)
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                f"Dataset source={data_status.get('active_source', 'live_logger')} "
                f"hours={data_status['data_hours']:.2f}, rows={data_status['rows_total']}, files={data_status['file_count']}"
            ),
        )
        if data_status["data_hours"] < config["min_data_hours"]:
            message = (
                f"Insufficient data for {pair_symbol}. Need {config['min_data_hours']}h, "
                f"have {data_status['data_hours']:.2f}h."
            )
            await self._repository.update_run(
                run_id,
                {
                    "status": "FAILED",
                    "stage": "data",
                    "stage_progress": 0.0,
                    "ended_at": utc_now_iso(),
                    "updated_at": utc_now_iso(),
                    "error_text": message,
                },
            )
            await self._emit_run_log(run_id, "ERROR", message)
            await self._emit_run_update(run_id)
            return

        await self._set_stage(run_id, "features", 0.2, "Building feature windows")
        await self._wait_if_paused(run_id)
        if control.stop:
            await self._mark_stopped(run_id, "Stopped before feature stage.")
            return
        if np is None:
            await self._mark_failed(run_id, "NumPy is required for dataset preparation.")
            return
        dataset = await self._build_dataset(pair_symbol, config, run_id)
        if dataset is None:
            await self._mark_failed(run_id, "Dataset build returned empty windows.")
            return

        await self._set_stage(run_id, "labeling", 0.35, "Preparing targets")
        await self._wait_if_paused(run_id)
        if control.stop:
            await self._mark_stopped(run_id, "Stopped before labeling stage.")
            return

        model_type_stage = str(((run.get("config", {}) or {}).get("training", {}) or {}).get("model_type", "lstm_attention")).strip().lower()
        if model_type_stage == "tcn":
            stage_label = "Training TCN model"
        elif model_type_stage == "transformer_encoder":
            stage_label = "Training Transformer Encoder model"
        else:
            stage_label = "Training LSTM + Attention model"
        await self._set_stage(run_id, "training", 0.45, stage_label)
        await self._wait_if_paused(run_id)
        if control.stop:
            await self._mark_stopped(run_id, "Stopped before training stage.")
            return
        train_result = await self._train_model(dataset, config, run_dir, run_id, pair_symbol, instance_id)
        if train_result is None:
            await self._mark_failed(run_id, "Training was not completed.")
            return

        await self._set_stage(run_id, "evaluation", 0.9, "Evaluating model performance")
        await self._wait_if_paused(run_id)
        if control.stop:
            await self._mark_stopped(run_id, "Stopped before evaluation stage.")
            return
        metrics = await self._evaluate_model(dataset, train_result, config, run_id, pair_symbol)
        metrics_path = run_dir / "metrics.json"
        metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
        await self._emit_run_log(run_id, "INFO", f"Evaluation complete: {json.dumps(metrics)}")
        run_after_eval = await self._repository.get_run(run_id)
        existing_metrics = run_after_eval.get("metrics", {}) if isinstance(run_after_eval, dict) else {}
        merged_metrics = dict(existing_metrics) if isinstance(existing_metrics, dict) else {}
        merged_metrics.update(metrics)
        try:
            feature_columns = dataset.get("feature_columns", []) if isinstance(dataset, dict) else []
            feature_schema_hash = hashlib.sha256(
                json.dumps([str(v) for v in feature_columns], separators=(",", ":"), ensure_ascii=True).encode("utf-8")
            ).hexdigest()
            config_snapshot = json.dumps(config, separators=(",", ":"), ensure_ascii=True)
            threshold_snapshot = {
                "recommended_confidence_threshold": merged_metrics.get("recommended_confidence_threshold"),
                "recommended_label_threshold_pct": merged_metrics.get("recommended_label_threshold_pct"),
                "minimum_action_rate_used": merged_metrics.get("minimum_action_rate_used"),
                "minimum_directional_samples_used": merged_metrics.get("minimum_directional_samples_used"),
            }
            merged_metrics["feature_schema_hash"] = feature_schema_hash
            merged_metrics["config_snapshot"] = config_snapshot
            merged_metrics["threshold_snapshot"] = threshold_snapshot
            merged_metrics["calibration_version"] = "temperature_scaling_v1"
            merged_metrics["model_hash"] = hashlib.sha256(
                (str(run_dir / "model.pt") + "|" + str(run_id)).encode("utf-8")
            ).hexdigest()
        except Exception:
            pass
        try:
            sweep_rows = merged_metrics.get("confidence_threshold_sweep", [])
            if isinstance(sweep_rows, list) and sweep_rows:
                sweep_artifact = {
                    "run_id": run_id,
                    "pair_symbol": pair_symbol,
                    "chosen_threshold": merged_metrics.get("recommended_confidence_threshold"),
                    "rejection_summary": {
                        "action_rate_rejections_count": merged_metrics.get("action_rate_rejections_count"),
                        "directional_rejections_count": merged_metrics.get("directional_rejections_count"),
                    },
                    "config_snapshot": {
                        "paper_bot": dict(config.get("paper_bot", {})) if isinstance(config.get("paper_bot"), dict) else {},
                        "target_mode": config.get("target_mode"),
                    },
                    "global_metrics": sweep_rows,
                    "regime_metrics": merged_metrics.get("regime_metrics", {}),
                }
                artifacts_dir = run_dir / "artifacts"
                artifacts_dir.mkdir(parents=True, exist_ok=True)
                (artifacts_dir / "confidence_sweep.json").write_text(
                    json.dumps(sweep_artifact, indent=2),
                    encoding="utf-8",
                )
        except Exception:
            pass

        await self._set_stage(run_id, "ready", 1.0, "Run completed. Awaiting manual approval")
        await self._repository.update_run(
            run_id,
            {
                "status": "COMPLETED",
                "stage": "ready",
                "stage_progress": 1.0,
                "ended_at": utc_now_iso(),
                "updated_at": utc_now_iso(),
                "metrics": merged_metrics,
                "config": config,
            },
        )
        await self._write_run_info_file(run_dir, run_id, pair_symbol, config, merged_metrics, train_result["device"])
        await self._emit_run_log(run_id, "INFO", "Run completed successfully. Manual approval required.")
        await self._emit_run_update(run_id)

    async def _set_stage(self, run_id: str, stage: str, progress: float, message: str) -> None:
        await self._repository.update_run(
            run_id,
            {
                "status": "RUNNING",
                "stage": stage,
                "stage_progress": progress,
                "updated_at": utc_now_iso(),
            },
        )
        await self._emit_run_log(run_id, "INFO", message)
        await self._emit_run_update(run_id)

    async def _wait_if_paused(self, run_id: str) -> None:
        while True:
            control = self._controls.setdefault(run_id, _RunControl())
            if control.stop:
                return
            if not control.pause:
                return
            await self._repository.update_run(
                run_id,
                {
                    "status": "PAUSED",
                    "updated_at": utc_now_iso(),
                },
            )
            await self._emit_run_update(run_id)
            await asyncio.sleep(0.5)

    async def _mark_stopped(self, run_id: str, message: str) -> None:
        await self._repository.update_run(
            run_id,
            {
                "status": "STOPPED",
                "stage": "stopped",
                "stage_progress": 0.0,
                "ended_at": utc_now_iso(),
                "updated_at": utc_now_iso(),
                "error_text": message,
            },
        )
        await self._emit_run_log(run_id, "INFO", message)
        await self._emit_run_update(run_id)

    async def _mark_failed(self, run_id: str, message: str) -> None:
        await self._repository.update_run(
            run_id,
            {
                "status": "FAILED",
                "stage": "failed",
                "stage_progress": 0.0,
                "ended_at": utc_now_iso(),
                "updated_at": utc_now_iso(),
                "error_text": message,
            },
        )
        await self._emit_run_log(run_id, "ERROR", message)
        await self._emit_run_update(run_id)

    async def _build_dataset(self, pair_symbol: str, config: dict[str, Any], run_id: str) -> dict[str, Any] | None:
        if np is None:
            await self._emit_run_log(run_id, "ERROR", "NumPy is required for training dataset pipeline.")
            return None
        dataset_source = str(config.get("dataset_source", "features_manifest") or "features_manifest").strip().lower()
        if dataset_source == "local_market_events":
            return await self._build_dataset_from_market_events(pair_symbol, config, run_id)

        use_local = bool(config.get("use_local_data", True))
        use_historic = bool(config.get("use_historic_data", False))
        if not use_local and not use_historic:
            await self._emit_run_log(run_id, "ERROR", "Both local and historic data sources are disabled in Pair Profile.")
            return None
        run = await self._repository.get_run(run_id)
        instance_id = str((run or {}).get("instance_id", "") or "")
        instance = await self._repository.get_instance(instance_id) if instance_id else None
        source_files: list[Path] = []
        source_labels: list[str] = []
        if use_local:
            manifests = await self._repository.manifests_for_pair(pair_symbol)
            local_files = [Path(item["file_path"]) for item in manifests]
            source_files.extend(local_files)
            source_labels.append(f"live_logger:{self._feature_data_root / pair_symbol}")
        if use_historic:
            source_root = self._historic_data_root(pair_symbol, instance.name if instance else "Default", instance_id)
            historic_files = self._source_feature_files_from_root(source_root)
            source_files.extend(historic_files)
            source_labels.append(f"historic_data_acquired:{source_root}")
        await self._emit_run_log(run_id, "INFO", f"Using dataset source(s): {' | '.join(source_labels)}")
        if not source_files:
            return None
        rows: list[dict[str, Any]] = []
        selected_exchanges = set(self._training_exchanges(config))
        dedupe: set[tuple[int, str]] = set()
        for file_path in source_files:
            if not file_path.exists():
                continue
            if file_path.suffix.lower() == ".parquet" and pq is None:
                await self._emit_run_log(
                    run_id,
                    "WARNING",
                    f"Skipping parquet file (pyarrow unavailable): {file_path.name}",
                )
                continue
            records = self._read_feature_records(file_path)
            if not records:
                continue
            for record in records:
                enabled = set(str(ex).lower() for ex in record.get("enabled_exchange_ids", []) if str(ex).strip())
                if selected_exchanges and enabled and not (enabled & selected_exchanges):
                    continue
                if selected_exchanges and not enabled:
                    continue
                ts_ms = int(record.get("ts_ms", 0) or 0)
                source_exchange = str(record.get("source_exchange_id") or (sorted(enabled)[0] if enabled else "unknown")).lower()
                key = (ts_ms, source_exchange)
                if key in dedupe:
                    continue
                dedupe.add(key)
                rows.append(record)
        shard_config = dict(config)
        shard_config["_shard_mode_builder"] = True
        lookback_cfg = int((shard_config.get("training", {}) or {}).get("lookback_steps", 0) or 0)
        horizon_cfg = int(max(shard_config.get("horizons", [0]) or [0]))
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                "Calling window builder: "
                f"source=feature_shards rows_registered={len(rows)} "
                f"lookback={lookback_cfg} horizon={horizon_cfg}"
            ),
        )
        return await self._build_dataset_from_feature_rows(rows, shard_config, run_id)

    def _canonical_run_dir_from_run(self, run: dict[str, Any]) -> Path:
        pair_symbol = str(run.get("pair_symbol", "UNKNOWN") or "UNKNOWN")
        instance_id = str(run.get("instance_id", "") or "")
        cfg = run.get("config", {}) if isinstance(run.get("config"), dict) else {}
        instance_name = str(cfg.get("instance_name", "") or "").strip()
        if not instance_name and instance_id:
            # Stable canonical fallback for this codebase.
            instance_name = f"{pair_symbol.upper()} Default"
        if not instance_name:
            instance_name = "Default"
        return self._run_dir(pair_symbol, run.get("run_id", ""), instance_name=instance_name, instance_id=instance_id)

    async def _write_data_health_report(self, run_id: str, report: dict[str, Any]) -> None:
        run = await self._repository.get_run(run_id)
        if not run:
            return
        run_dir = self._canonical_run_dir_from_run(run)
        run_dir.mkdir(parents=True, exist_ok=True)
        json_path = run_dir / "data_health_report.json"
        txt_path = run_dir / "data_health_report.txt"
        json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        lines = [
            f"data_health_ok={bool(report.get('data_health_ok', False))}",
            f"fail_reasons={','.join(report.get('fail_reasons', []))}",
            f"coverage={json.dumps(report.get('coverage', {}), ensure_ascii=True)}",
            f"event_mix={json.dumps(report.get('event_mix', {}), ensure_ascii=True)}",
            f"price_sanity={json.dumps(report.get('price_sanity', {}), ensure_ascii=True)}",
            f"orderbook_sanity={json.dumps(report.get('orderbook_sanity', {}), ensure_ascii=True)}",
            f"trade_flow_sanity={json.dumps(report.get('trade_flow_sanity', {}), ensure_ascii=True)}",
            f"feature_sanity={json.dumps(report.get('feature_sanity', {}), ensure_ascii=True)}",
            f"label_sanity={json.dumps(report.get('label_sanity', {}), ensure_ascii=True)}",
            f"label_probe_summary={json.dumps(report.get('label_probe_summary', {}), ensure_ascii=True)}",
        ]
        txt_path.write_text("\n".join(lines), encoding="utf-8")

    def _build_data_health_report(
        self,
        *,
        chunk_rows: list[dict[str, Any]],
        events_scanned: int,
        events_after_hour_filter: int,
        aggregated_feature_stats: dict[str, int],
        dataset: dict[str, Any] | None,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        coverage_start = int(chunk_rows[0].get("ts_start_ms", 0) or 0) if chunk_rows else 0
        coverage_end = int(chunk_rows[-1].get("ts_end_ms", 0) or 0) if chunk_rows else 0
        coverage_hours = max(0.0, float(coverage_end - coverage_start) / 3_600_000.0) if coverage_end > coverage_start else 0.0
        gaps_1s = gaps_5s = gaps_30s = gaps_60s = 0
        prev_end = 0
        for row in chunk_rows:
            s = int(row.get("ts_start_ms", 0) or 0)
            e = int(row.get("ts_end_ms", 0) or 0)
            if prev_end > 0 and s > prev_end:
                gap = s - prev_end
                if gap > 1_000:
                    gaps_1s += 1
                if gap > 5_000:
                    gaps_5s += 1
                if gap > 30_000:
                    gaps_30s += 1
                if gap > 60_000:
                    gaps_60s += 1
            prev_end = max(prev_end, e)
        windows_total = int((dataset or {}).get("windows_total", 0) or 0)
        train_w = int((dataset or {}).get("train_size", 0) or 0)
        val_w = int((dataset or {}).get("val_size", 0) or 0)
        test_w = int((dataset or {}).get("test_size", 0) or 0)
        stale_rows = int((dataset or {}).get("stale_rows", 0) or 0)
        extraction_trace = (dataset or {}).get("feature_extraction_trace", {}) if isinstance((dataset or {}).get("feature_extraction_trace"), dict) else {}
        window_trace = (dataset or {}).get("window_assembly_trace", {}) if isinstance((dataset or {}).get("window_assembly_trace"), dict) else {}
        feature_rows = int(
            (extraction_trace.get("feature_rows_registered_for_dataset", 0) or 0)
            or (extraction_trace.get("feature_shard_rows_total", 0) or 0)
            or (aggregated_feature_stats.get("rows_emitted", 0) or 0)
        )
        windows_attempted = int(window_trace.get("windows_attempted", 0) or 0)
        windows_final = int(window_trace.get("windows_final", 0) or 0)
        if windows_total <= 0:
            windows_total = int(windows_final)
        stale_rate = (float(stale_rows) / float(max(1, feature_rows))) if feature_rows > 0 else 0.0
        target_mode = str((dataset or {}).get("target_mode", "trade_outcome") or "trade_outcome").strip().lower()
        def _dist(arr: Any) -> dict[str, Any]:
            try:
                if np is None:
                    return {}
                a = np.asarray(arr, dtype=np.int64)
                if a.size <= 0:
                    return {}
                labels = ["short_good", "no_trade", "long_good"] if target_mode == "trade_outcome" else ["down", "flat", "up"]
                out: dict[str, Any] = {}
                n = int(a.size)
                for i, lbl in enumerate(labels):
                    c = int(np.sum(a == i))
                    out[lbl] = {"count": c, "pct": float(c / max(1, n))}
                return out
            except Exception:
                return {}
        train_labels = _dist((dataset or {}).get("y_dir_train", []))
        val_labels = _dist((dataset or {}).get("y_dir_val", []))
        test_labels = _dist((dataset or {}).get("y_dir_test", []))

        feature_names = list((dataset or {}).get("feature_names", FEATURE_COLUMNS) or FEATURE_COLUMNS)
        raw_type_audit = (dataset or {}).get("raw_event_type_audit", {}) if isinstance((dataset or {}).get("raw_event_type_audit"), dict) else {}
        intentionally_unsupported = {
            "liq_events_60s", "liq_count_60s", "liq_notional_60s", "liq_buy_sell_imbalance", "liq_momentum",
            "open_interest_change_pct", "oi_velocity", "funding_rate", "funding_rate_change",
        }
        zero_rate_by_feature: dict[str, float] = {}
        null_rate_by_feature: dict[str, float] = {}
        constant_features_active: list[str] = []
        constant_features_unsupported: list[str] = []
        nfeat = len(feature_names)
        if nfeat > 0:
            norm_path = str((dataset or {}).get("normalized_features_path", "") or "")
            try:
                norm_arr = np.load(norm_path, mmap_mode="r") if (np is not None and norm_path) else None
            except Exception:
                norm_arr = None
            if norm_arr is not None and getattr(norm_arr, "ndim", 0) == 2 and int(norm_arr.shape[1]) > 0:
                for i, fname in enumerate(feature_names):
                    if i >= int(norm_arr.shape[1]):
                        break
                    col = np.asarray(norm_arr[:, i], dtype=np.float64)
                    if col.size == 0:
                        zr = 1.0
                        nr = 1.0
                        is_const = True
                    else:
                        nr = float(np.mean(~np.isfinite(col)))
                        finite = col[np.isfinite(col)]
                        zr = float(np.mean(np.isclose(finite, 0.0))) if finite.size else 1.0
                        is_const = bool(finite.size == 0 or np.nanstd(finite) <= 1e-12)
                    zero_rate_by_feature[fname] = zr
                    null_rate_by_feature[fname] = nr
                    if is_const:
                        if fname in intentionally_unsupported:
                            constant_features_unsupported.append(fname)
                        else:
                            constant_features_active.append(fname)

        total_windows = max(1, train_w + val_w + test_w)
        split_share = {
            "train": float(train_w / total_windows),
            "val": float(val_w / total_windows),
            "test": float(test_w / total_windows),
        }
        trade_total = int(aggregated_feature_stats.get("events_trade", 0) or 0)
        bookticker_total = int(aggregated_feature_stats.get("events_bookticker", 0) or 0)
        depth_total = int(aggregated_feature_stats.get("events_depth", 0) or 0)
        coverage_minutes = max(1.0, coverage_hours * 60.0)
        flow_by_split: dict[str, Any] = {}
        for split_name, share in split_share.items():
            sp_minutes = max(1.0, coverage_minutes * share)
            tr = int(round(trade_total * share))
            bt = int(round(bookticker_total * share))
            dp = int(round(depth_total * share))
            flow_by_split[split_name] = {
                "trade_events_per_min": float(tr / sp_minutes),
                "trade_events_per_hour": float((tr / sp_minutes) * 60.0),
                "bookticker_events_per_min": float(bt / sp_minutes),
                "bookticker_events_per_hour": float((bt / sp_minutes) * 60.0),
                "depth_events_per_min": float(dp / sp_minutes),
                "depth_events_per_hour": float((dp / sp_minutes) * 60.0),
            }

        trade_age_ms = {
            "train": {"p50": None, "p90": None, "p99": None},
            "val": {"p50": None, "p90": None, "p99": None},
            "test": {"p50": None, "p90": None, "p99": None},
        }
        windows_no_recent_trade_pct = {
            "train": None,
            "val": None,
            "test": None,
        }

        split_stale_rates = {
            "train": float((dataset or {}).get("split_stale_rates", {}).get("train", 0.0) or 0.0),
            "val": float((dataset or {}).get("split_stale_rates", {}).get("val", 0.0) or 0.0),
            "test": float((dataset or {}).get("split_stale_rates", {}).get("test", 0.0) or 0.0),
        }

        normalized_counts = raw_type_audit.get("normalized_event_type_counts", {}) if isinstance(raw_type_audit.get("normalized_event_type_counts", {}), dict) else {}
        normalized_trade_count = int(normalized_counts.get("trade", 0) or 0)
        normalized_bbo_count = int(normalized_counts.get("bookticker", 0) or 0)
        normalized_depth_count = int(normalized_counts.get("depth", 0) or 0)
        fail_reasons: list[str] = []
        if events_after_hour_filter <= 0:
            fail_reasons.append("missing_event_types")
        if gaps_60s > 0:
            fail_reasons.append("large_time_gaps")
        if stale_rate > 0.25:
            fail_reasons.append("stale_book_data")
        if feature_rows <= 0:
            fail_reasons.append("too_many_constant_features")
        if windows_total <= 0:
            fail_reasons.append("label_distribution_unstable")
        trade_feature_keys = ["trade_rate_10s", "market_buy_volume_10s", "market_sell_volume_10s", "aggressive_buy_sell_delta", "volume_10s"]
        trade_feature_present = any(float(zero_rate_by_feature.get(k, 1.0)) < 1.0 for k in trade_feature_keys)
        if normalized_trade_count <= 0:
            if feature_rows > 0 and (normalized_bbo_count > 0 or normalized_depth_count > 0 or trade_feature_present):
                fail_reasons.append("trade_event_type_mapping_missing")
            else:
                fail_reasons.append("insufficient_trade_flow")
        # Refine constant-feature failure to active model features only.
        if "too_many_constant_features" in fail_reasons and len(constant_features_active) == 0:
            fail_reasons = [x for x in fail_reasons if x != "too_many_constant_features"]
        # Refine label instability: if split distributions exist and windows exist, do not flag.
        if "label_distribution_unstable" in fail_reasons and windows_total > 0 and train_labels and val_labels and test_labels:
            fail_reasons = [x for x in fail_reasons if x != "label_distribution_unstable"]
        data_health_ok = len(fail_reasons) == 0
        summary = (
            f"coverage_hours={coverage_hours:.2f} scanned={events_scanned} filtered={events_after_hour_filter} "
            f"feature_rows={feature_rows} windows_total={windows_total} windows_attempted={windows_attempted} stale_rate={stale_rate:.4f}"
        )
        recorder_problem = bool("large_time_gaps" in fail_reasons)
        feature_extraction_problem = bool(
            "too_many_constant_features" in fail_reasons
            or "insufficient_trade_flow" in fail_reasons
            or "trade_event_type_mapping_missing" in fail_reasons
            or "feature_trade_fields_missing" in fail_reasons
        )
        label_config_problem = bool("label_distribution_unstable" in fail_reasons)
        insufficient_market_activity = bool(
            ("insufficient_trade_flow" in fail_reasons or "trade_event_type_mapping_missing" in fail_reasons)
            and coverage_hours > 0 and normalized_trade_count < max(100, int(coverage_hours * 30))
        )
        trigger_threshold = max(100, int(coverage_hours * 30)) if coverage_hours > 0 else 100
        return {
            "data_health_ok": data_health_ok,
            "fail_reasons": fail_reasons,
            "coverage": {
                "start_ts_ms": coverage_start,
                "end_ts_ms": coverage_end,
                "total_hours": coverage_hours,
                "gaps_gt_1s": gaps_1s,
                "gaps_gt_5s": gaps_5s,
                "gaps_gt_30s": gaps_30s,
                "gaps_gt_60s": gaps_60s,
            },
            "event_mix": {
                "events_scanned": int(events_scanned),
                "events_after_hour_filter": int(events_after_hour_filter),
                "bookticker_count": int(aggregated_feature_stats.get("events_bookticker", 0) or 0),
                "depth_count": int(aggregated_feature_stats.get("events_depth", 0) or 0),
                "trade_count": int(aggregated_feature_stats.get("events_trade", 0) or 0),
                "liquidation_count": int(aggregated_feature_stats.get("events_liq", 0) or 0),
                "open_interest_count": int(aggregated_feature_stats.get("events_mark", 0) or 0),
                "funding_count": int(aggregated_feature_stats.get("events_mark", 0) or 0),
            },
            "price_sanity": {"abnormal_jump_count": 0, "duplicate_timestamp_count": 0, "non_monotonic_timestamp_count": 0},
            "orderbook_sanity": {"ask_lt_bid_count": 0, "crossed_book_count": 0, "spread_bps_p50": 0.0, "spread_bps_p90": 0.0, "spread_bps_p99": 0.0, "spread_bps_max": 0.0},
            "trade_flow_sanity": {
                "trade_count_total": int(aggregated_feature_stats.get("events_trade", 0) or 0),
                "flow_by_split": flow_by_split,
                "windows_no_recent_trade_pct_by_split": windows_no_recent_trade_pct,
                "trade_age_ms_by_split": trade_age_ms,
                "trade_flow_missing_vs_extraction": (
                    "extraction_counting_problem_suspected" if normalized_trade_count <= 0 and events_after_hour_filter > 0 else "activity_or_source_limited"
                ),
                "insufficient_trade_flow_rule": {
                    "threshold_value": int(trigger_threshold),
                    "measured_trade_events_overall": int(normalized_trade_count),
                    "measured_trade_events_legacy_counter": int(trade_total),
                    "reason_scope": "overall",
                },
            },
            "raw_event_type_candidates": raw_type_audit.get("raw_event_type_candidates", {}),
            "raw_event_type_unique_values": raw_type_audit.get("raw_event_type_unique_values", {}),
            "normalized_event_type_counts": raw_type_audit.get("normalized_event_type_counts", {}),
            "event_type_mapping_used": raw_type_audit.get("event_type_mapping_used", {}),
            "unmapped_event_type_counts": raw_type_audit.get("unmapped_event_type_counts", {}),
            "feature_sanity": {
                "rows_emitted": feature_rows,
                "stale_rows": stale_rows,
                "stale_rate": stale_rate,
                "dropped_window_rate": float((dataset or {}).get("dropped_window_rate", 0.0) or 0.0),
                "split_stale_rates": split_stale_rates,
                "constant_features_active": constant_features_active,
                "constant_features_unsupported": constant_features_unsupported,
                "zero_rate_by_feature": zero_rate_by_feature,
                "null_rate_by_feature": null_rate_by_feature,
                "disabled_or_unsupported_features": sorted(list(intentionally_unsupported)),
            },
            "label_sanity": {
                "train": train_labels,
                "val": val_labels,
                "test": test_labels,
                "train_windows": train_w,
                "val_windows": val_w,
                "test_windows": test_w,
                "by_day": {},
                "by_session": {},
                "unstable_days_or_sessions": [],
            },
            "label_probe_summary": (dataset or {}).get("label_probe_summary", {}) if isinstance((dataset or {}).get("label_probe_summary"), dict) else {},
            "data_health_summary": summary,
            "actionable_verdict": {
                "recorder_problem": recorder_problem,
                "feature_extraction_problem": feature_extraction_problem,
                "label_config_problem": label_config_problem,
                "insufficient_market_activity": insufficient_market_activity,
            },
        }

    async def _build_dataset_from_market_events(self, pair_symbol: str, config: dict[str, Any], run_id: str) -> dict[str, Any] | None:
        if pq is None:
            await self._emit_run_log(run_id, "ERROR", "pyarrow is required for local_market_events dataset source.")
            return None
        selected_exchanges = {str(item).lower() for item in self._training_exchanges(config)}
        chunk_rows: list[dict[str, Any]] = []
        chunk_page_size = max(1000, int(os.getenv("ML_MARKET_EVENTS_CHUNK_PAGE_SIZE", "5000")))
        chunk_offset = 0
        while True:
            page = self._list_market_event_chunks(
                pair_symbol=pair_symbol,
                limit=chunk_page_size,
                offset=chunk_offset,
            )
            if not page:
                break
            chunk_rows.extend(page)
            chunk_offset += len(page)
            if len(page) < chunk_page_size:
                break
        if not chunk_rows:
            await self._emit_run_log(run_id, "WARNING", f"No market event chunks found for {pair_symbol}.")
            return None
        chunks_total = int(len(chunk_rows))
        total_data_bytes = int(sum(int(item.get("size_bytes", 0) or 0) for item in chunk_rows))
        # Training usually uses a subset of exchanges; prune chunk list early to avoid
        # spending time decoding parquet batches we will discard row-by-row later.
        if selected_exchanges:
            chunk_rows_selected = [
                item
                for item in chunk_rows
                if str(item.get("source_exchange_id", "") or "").strip().lower() in selected_exchanges
            ]
            if chunk_rows_selected:
                selected_bytes = int(sum(int(item.get("size_bytes", 0) or 0) for item in chunk_rows_selected))
                await self._emit_run_log(
                    run_id,
                    "INFO",
                    (
                        "Planner exchange filter applied: "
                        f"selected_exchanges={','.join(sorted(selected_exchanges))} "
                        f"chunks={len(chunk_rows_selected)}/{chunks_total} "
                        f"data_gb={selected_bytes / float(1024**3):.3f}/{total_data_bytes / float(1024**3):.3f}"
                    ),
                )
                chunk_rows = chunk_rows_selected
                chunks_total = int(len(chunk_rows))
                total_data_bytes = selected_bytes
        total_data_gb = float(total_data_bytes / float(1024**3)) if total_data_bytes > 0 else 0.0
        config["_planner_total_data_bytes"] = int(total_data_bytes)
        config["_planner_chunks_total"] = int(chunks_total)
        ram_budget_bytes = self._training_memory_budget_bytes(config=config)
        ram_budget_gb = float(ram_budget_bytes / float(1024**3))
        target_bytes = int(max(64 * 1024 * 1024, ram_budget_bytes * 0.85))
        hard_guard_bytes = int(max(96 * 1024 * 1024, ram_budget_bytes * 0.95))
        # Keep a safety margin below the visible budget because process RSS can jump
        # between checks due to Arrow allocations/fragmentation.
        strict_rss_cap_bytes = int(max(96 * 1024 * 1024, ram_budget_bytes * 0.90))
        prefetch_enabled = bool(config.get("training_prefetch_enabled", False))
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                f"Market-event planner: chunks={chunks_total} (page_size={chunk_page_size}) "
                f"range=[{int(chunk_rows[0].get('ts_start_ms', 0) or 0)}..{int(chunk_rows[-1].get('ts_end_ms', 0) or 0)}] "
                f"total_data_gb={total_data_gb:.3f} ram_budget_gb={ram_budget_gb:.3f} "
                f"target_gb={target_bytes / float(1024**3):.3f} hard_guard_gb={hard_guard_bytes / float(1024**3):.3f} "
                f"prefetch={prefetch_enabled}"
            ),
        )
        await self._emit_run_log(run_id, "INFO", f"Using dataset source(s): local_market_events:{self._market_events_root / f'pair={pair_symbol}'}")
        await self._merge_run_metrics(
            run_id,
            {
                "used_data_gb": 0.0,
                "total_data_gb": float(round(total_data_gb, 6)),
                "loaded_now_gb": 0.0,
                "process_rss_gb": 0.0,
                "ram_budget_gb": float(round(ram_budget_gb, 6)),
                "chunks_processed": 0,
                "chunks_total": chunks_total,
                "data_chunks_processed": 0,
                "data_chunks_total": chunks_total,
                "windows_processed": 0,
                "windows_total": 0,
                "training_prefetch_enabled": prefetch_enabled,
                "resumed_from_checkpoint": False,
                "checkpoint_step": "",
            },
            emit_update=True,
        )
        full_data_mode = bool(config.get("full_data_mode", True))
        max_events_env = int(os.getenv("ML_MARKET_EVENTS_MAX_EVENTS", "0"))
        if full_data_mode:
            max_events_env = 0
        effective_sample_ms = max(100, int(100 if full_data_mode else int(os.getenv("ML_MARKET_EVENT_SAMPLE_MS", "200"))))
        config["_effective_sample_ms"] = int(effective_sample_ms)
        await self._emit_run_log(run_id, "INFO", f"Effective market-event sampling: sample_ms={effective_sample_ms}")

        anchor_starts = [
            int(item.get("ts_start_ms", 0) or 0)
            for item in chunk_rows
            if int(item.get("ts_start_ms", 0) or 0) > 0
        ]
        anchor_ends = [
            int(item.get("ts_end_ms", 0) or 0)
            for item in chunk_rows
            if int(item.get("ts_end_ms", 0) or 0) > 0
        ]
        anchor_min_ts = min(anchor_starts) if anchor_starts else 0
        anchor_max_ts = max(anchor_ends) if anchor_ends else 0
        hour_window_enabled = bool(config.get("training_hour_window_enabled", False))
        requested_start_hour = max(0, int(config.get("training_hour_start", 0) or 0))
        requested_end_hour = max(0, int(config.get("training_hour_end", 0) or 0))
        effective_end_hour = requested_end_hour
        start_ts_ms: int | None = None
        end_ts_ms: int | None = None
        if hour_window_enabled and anchor_min_ts > 0 and anchor_max_ts >= anchor_min_ts:
            if requested_end_hour != 0 and requested_end_hour < requested_start_hour:
                requested_end_hour = requested_start_hour
            start_ts_ms = anchor_min_ts + (requested_start_hour * 3_600_000)
            if requested_end_hour == 0:
                end_ts_ms = anchor_max_ts
                effective_end_hour = max(requested_start_hour, int((anchor_max_ts - anchor_min_ts) // 3_600_000))
            else:
                end_ts_ms = anchor_min_ts + ((requested_end_hour + 1) * 3_600_000) - 1
                effective_end_hour = requested_end_hour
            await self._emit_run_log(
                run_id,
                "INFO",
                (
                    "Training hour window enabled "
                    f"(start={requested_start_hour}, end={requested_end_hour}, "
                    f"effective_start={requested_start_hour}, effective_end={effective_end_hour}, "
                    f"start_ts={start_ts_ms}, end_ts={end_ts_ms})"
                ),
            )
        else:
            await self._emit_run_log(run_id, "INFO", "Training hour window disabled. Using full local_market_events range.")

        cache_enabled = bool(config.get("full_data_mode", True))
        cache_key = self._market_events_feature_cache_key(
            pair_symbol=pair_symbol,
            config=config,
            chunk_rows=chunk_rows,
            effective_sample_ms=effective_sample_ms,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
        )
        cache_file = self._dataset_cache_root / f"{str(pair_symbol).upper()}_{cache_key}.jsonl"
        if cache_enabled and cache_file.exists():
            try:
                # Local market-event mode must remain shard-manifest based; skip cache materialization.
                await self._emit_run_log(
                    run_id,
                    "INFO",
                    (
                        "Dataset feature cache exists but is bypassed for local_market_events "
                        f"(key={cache_key[:12]} file={cache_file.name}) to keep shard-manifest memory mode."
                    ),
                )
            except Exception as exc:
                await self._emit_run_log(run_id, "WARNING", f"Dataset feature cache read failed; rebuilding ({exc}).")

        feature_rows_total = 0
        feature_windows_total = 0
        feature_shard_entries: list[dict[str, Any]] = []
        carry_events: list[dict[str, Any]] = []
        group_events: list[dict[str, Any]] = []
        group_chunks = 0
        group_started_at = time.monotonic()
        carry_ms = max(120_000, (int(config["training"]["lookback_steps"]) + int(max(config["horizons"])) + 1) * effective_sample_ms)
        last_feature_ts = 0
        events_scanned = 0
        events_after_hour_filter = 0
        processed_chunks = 0
        bytes_scanned = 0
        last_forced_metrics_emit = time.monotonic()
        last_progress_monotonic = time.monotonic()
        aggregated_feature_stats: dict[str, int] = {
            "events_total": 0,
            "events_tool_filtered": 0,
            "events_json_error": 0,
            "events_bookticker": 0,
            "events_depth": 0,
            "events_trade": 0,
            "events_mark": 0,
            "events_liq": 0,
            "skip_no_mid": 0,
            "skip_sample_gate": 0,
            "rows_emitted": 0,
        }
        extraction_trace: dict[str, int] = {
            "scanned_events": 0,
            "after_exchange_filter": 0,
            "after_event_type_filter": 0,
            "after_symbol_filter": 0,
            "after_timestamp_validity_filter": 0,
            "events_bookticker": 0,
            "events_depth": 0,
            "events_trade": 0,
            "events_liq": 0,
            "events_mark": 0,
            "events_unknown": 0,
            "grid_rows_attempted": 0,
            "rows_with_valid_book": 0,
            "rows_with_valid_spread": 0,
            "rows_with_recent_trade": 0,
            "rows_rejected_stale_book": 0,
            "rows_rejected_stale_trade": 0,
            "rows_rejected_spread": 0,
            "rows_rejected_missing_price": 0,
            "rows_rejected_missing_features": 0,
            "feature_rows_emitted": 0,
            "feature_shards_written": 0,
            "feature_shard_rows_total": 0,
            "feature_manifest_rows": 0,
            "feature_manifest_paths_count": 0,
            "feature_rows_registered_for_dataset": 0,
            "windows_attempted": 0,
            "windows_final": 0,
        }
        raw_type_candidates = ["event_type", "type", "e", "stream", "topic", "channel", "source"]
        raw_type_unique_values: dict[str, dict[str, int]] = {k: {} for k in raw_type_candidates}
        normalized_event_type_counts: dict[str, int] = {"bookticker": 0, "trade": 0, "depth": 0, "mark": 0, "liquidation": 0, "unknown": 0}
        unmapped_event_type_counts: dict[str, int] = {}
        raw_sample_limit = 2000
        raw_sample_seen = 0
        event_type_mapping_used = {
            "bookticker": ["bookticker", "book_ticker", "bookTicker", "bbo", "ticker"],
            "trade": ["trade", "aggtrade", "agg_trade", "market_trade", "trade_batch"],
            "depth": ["depth", "depthupdate", "orderbook", "book_depth", "external_depth"],
        }

        memory_pressure_stepdowns = 0
        first_flush_done = False
        rss_check_stride = 1
        runtime_forced_chunk_group_size: int | None = None
        use_chunk_worker = str(os.getenv("ML_MARKET_CHUNK_SUBPROCESS", "1")).strip().lower() not in {"0", "false", "off", "no"}
        chunk_worker_timeout_s = max(10, int(os.getenv("ML_MARKET_CHUNK_SUBPROCESS_TIMEOUT_SEC", "240")))
        chunk_worker_tmp_root = self._models_root / "_tmp" / "market_chunk_extract" / str(run_id)
        manifest_path = chunk_worker_tmp_root / "training_manifest.jsonl"
        feature_shard_dir = chunk_worker_tmp_root / "feature_shards"
        keep_training_shards = bool(config.get("keep_training_shards", False))
        recycle_enabled = bool(config.get("training_process_recycle_enabled", True))
        worker_groups_before_restart = int(max(1, int(config.get("training_worker_groups_before_restart", 1) or 1)))
        worker_memory_cap_override_gb = self._normalize_training_ram_budget_gb(config.get("training_worker_memory_cap_gb"))
        worker_cap_bytes = int((worker_memory_cap_override_gb or (strict_rss_cap_bytes / float(1024**3))) * (1024**3))
        if use_chunk_worker:
            chunk_worker_tmp_root.mkdir(parents=True, exist_ok=True)
            feature_shard_dir.mkdir(parents=True, exist_ok=True)
        await self._emit_run_log(
            run_id,
            "INFO",
            f"Chunk streaming enabled: worker_subprocess={use_chunk_worker} chunks_total={chunks_total}",
        )
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                f"Process recycle config: enabled={recycle_enabled} "
                f"groups_before_restart={worker_groups_before_restart} "
                f"worker_memory_cap_gb={worker_cap_bytes / float(1024**3):.3f}"
            ),
        )
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                f"Shard-manifest streaming enabled: manifest={manifest_path} "
                f"keep_training_shards={keep_training_shards}"
            ),
        )
        feature_stage_started = time.monotonic()
        feature_stage_timeout_s = max(300, int(os.getenv("ML_FEATURE_STAGE_TIMEOUT_SEC", "5400")))
        strict_cap_recovery_attempted = False
        for chunk_index, chunk in enumerate(chunk_rows, start=1):
            # Proof-run hard timeout: if we cannot reach manifest registration, fail with persisted evidence.
            if (time.monotonic() - feature_stage_started) >= feature_stage_timeout_s:
                extraction_trace["feature_manifest_paths_count"] = int(len(feature_shard_entries))
                extraction_trace["feature_rows_emitted"] = int(aggregated_feature_stats.get("rows_emitted", 0) or 0)
                fail_report = self._build_data_health_report(
                    chunk_rows=chunk_rows,
                    events_scanned=events_scanned,
                    events_after_hour_filter=events_after_hour_filter,
                    aggregated_feature_stats=aggregated_feature_stats,
                    dataset=None,
                    config=config,
                )
                fail_report["feature_extraction_trace"] = extraction_trace
                fail_report["data_health_ok"] = False
                fail_reasons = list(fail_report.get("fail_reasons", []))
                if "feature_stage_did_not_reach_manifest_registration" not in fail_reasons:
                    fail_reasons.append("feature_stage_did_not_reach_manifest_registration")
                fail_report["fail_reasons"] = fail_reasons
                await self._write_data_health_report(run_id, fail_report)
                await self._merge_run_metrics(
                    run_id,
                    {
                        "feature_extraction_trace": extraction_trace,
                        "data_health_ok": False,
                        "data_health_fail_reasons": fail_reasons,
                        "data_health_summary": str(fail_report.get("data_health_summary", "")),
                        "zero_row_root_cause": "feature_stage_did_not_reach_manifest_registration",
                        "recommended_live_mode": "HOLD_ONLY",
                        "model_result_judgeable": False,
                        "model_result_block_reason": "data_health_failure",
                    },
                    emit_update=True,
                )
                await self._emit_run_log(run_id, "ERROR", f"feature_stage_did_not_reach_manifest_registration timeout_s={feature_stage_timeout_s}")
                return None
            now_mono = time.monotonic()
            # Force telemetry even when chunk parsing is slow/stalled so UI/logs stay truthful.
            if now_mono - last_forced_metrics_emit >= 2.0:
                rss_tree_now = self._process_tree_resident_memory_bytes()
                await self._merge_run_metrics(
                    run_id,
                    {
                        "chunks_processed": int(processed_chunks),
                        "data_chunks_processed": int(processed_chunks),
                        "loaded_now_gb": float(round((rss_tree_now or 0) / float(1024**3), 6)),
                        "process_rss_gb": float(round((rss_tree_now or 0) / float(1024**3), 6)),
                    },
                    emit_update=True,
                )
                last_forced_metrics_emit = now_mono
            # Stall guard: do not allow silent stuck loops while memory keeps rising.
            if now_mono - last_progress_monotonic >= 300.0:
                await self._emit_run_log(
                    run_id,
                    "ERROR",
                    "Feature build stalled for 300s without chunk progress; aborting run for safety.",
                )
                return None
            if chunk_index == 1 or (chunk_index % rss_check_stride == 0):
                rss_now = self._process_tree_resident_memory_bytes()
                if rss_now is not None and rss_now >= strict_rss_cap_bytes:
                    if not strict_cap_recovery_attempted:
                        strict_cap_recovery_attempted = True
                        await self._emit_run_log(
                            run_id,
                            "WARNING",
                            (
                                f"High baseline RSS detected before chunk processing: "
                                f"rss_gb={rss_now / float(1024**3):.3f} >= ram_budget_gb={strict_rss_cap_bytes / float(1024**3):.3f}. "
                                "Attempting forced boundary cleanup once before failing."
                            ),
                        )
                        await self._runtime_boundary_cleanup(run_id=run_id, reason="strict_cap_recovery", force=True)
                        rss_now = self._process_tree_resident_memory_bytes()
                        if rss_now is not None and rss_now < strict_rss_cap_bytes:
                            await self._emit_run_log(
                                run_id,
                                "INFO",
                                (
                                    f"Strict-cap recovery succeeded: rss_gb={rss_now / float(1024**3):.3f} "
                                    f"< ram_budget_gb={strict_rss_cap_bytes / float(1024**3):.3f}. Continuing."
                                ),
                            )
                            continue
                    await self._emit_run_log(
                        run_id,
                        "ERROR",
                        (
                            f"STRICT RAM cap exceeded during full-fidelity feature build: "
                            f"rss_gb={rss_now / float(1024**3):.3f} >= ram_budget_gb={strict_rss_cap_bytes / float(1024**3):.3f}. "
                            "Stopping to respect Training RAM Budget. "
                            "Reduce data window, increase sample_ms, or increase RAM budget."
                        ),
                    )
                    await self._merge_run_metrics(
                        run_id,
                        {
                            "loaded_now_gb": float(round(rss_now / float(1024**3), 6)),
                            "process_rss_gb": float(round(rss_now / float(1024**3), 6)),
                            "ram_guard_triggered": True,
                        },
                        emit_update=True,
                    )
                    return None
            file_path = Path(str(chunk.get("file_path", "")))
            if not file_path.exists():
                continue
            chunk_events: list[dict[str, Any]] = []
            try:
                if use_chunk_worker:
                    parent_rss_before_worker = self._process_tree_resident_memory_bytes() or 0
                    await self._emit_run_log(
                        run_id,
                        "INFO",
                        f"Parent RSS before worker={parent_rss_before_worker / float(1024**3):.3f}GB",
                    )
                    out_path = chunk_worker_tmp_root / f"chunk_{chunk_index:06d}.jsonl"
                    ctx = mp.get_context("spawn")
                    q: Any = ctx.Queue(maxsize=1)
                    proc = ctx.Process(
                        target=_market_chunk_extract_worker_entry,
                        args=(
                            q,
                            str(file_path),
                            sorted(selected_exchanges),
                            start_ts_ms,
                            end_ts_ms,
                            str(out_path),
                        ),
                    )
                    proc.start()
                    job_handle = self._attach_windows_job_memory_cap(
                        int(proc.pid or 0),
                        int(worker_cap_bytes),
                    )
                    if job_handle and (chunk_index == 1 or chunk_index % 5 == 0):
                        await self._emit_run_log(
                            run_id,
                            "INFO",
                            f"Chunk worker job-cap attached: chunk={chunk_index}/{chunks_total} pid={proc.pid} cap_gb={worker_cap_bytes / float(1024**3):.3f}",
                        )
                    if chunk_index == 1 or chunk_index % 5 == 0:
                        await self._emit_run_log(
                            run_id,
                            "INFO",
                            f"Chunk worker started: chunk={chunk_index}/{chunks_total} pid={proc.pid} timeout_s={chunk_worker_timeout_s}",
                        )
                    proc.join(timeout=chunk_worker_timeout_s)
                    if proc.is_alive():
                        proc.kill()
                        proc.join(timeout=5)
                        raise RuntimeError(f"chunk_worker_timeout chunk={chunk_index} file={file_path.name}")
                    result = q.get_nowait() if not q.empty() else {"ok": False, "error": "chunk_worker_no_result"}
                    if not bool(result.get("ok", False)):
                        raise RuntimeError(str(result.get("error", "chunk_worker_error")))
                    if chunk_index == 1 or chunk_index % 5 == 0:
                        await self._emit_run_log(
                            run_id,
                            "INFO",
                            f"Chunk worker finished: chunk={chunk_index}/{chunks_total} rows={int(result.get('rows', 0) or 0)} scanned={int(result.get('events_scanned', 0) or 0)}",
                        )
                    if job_handle:
                        try:
                            ctypes.windll.kernel32.CloseHandle(job_handle)  # type: ignore[attr-defined]
                        except Exception:
                            pass
                    worker_peak_rss = int(result.get("worker_peak_rss_bytes", 0) or 0)
                    if worker_peak_rss > 0:
                        await self._emit_run_log(
                            run_id,
                            "INFO",
                            f"Worker RSS peak={worker_peak_rss / float(1024**3):.3f}GB",
                        )
                    events_scanned += int(result.get("events_scanned", 0) or 0)
                    extraction_trace["scanned_events"] += int(result.get("events_scanned", 0) or 0)
                    shard_row_count = int(result.get("rows", 0) or 0)
                    shard_size_mb = 0.0
                    if out_path.exists():
                        try:
                            shard_size_mb = float(out_path.stat().st_size / float(1024**2))
                        except Exception:
                            shard_size_mb = 0.0
                    shard_meta = {
                        "shard_id": f"shard_{chunk_index:06d}",
                        "shard_path": str(out_path),
                        "row_count": shard_row_count,
                        "window_count": 0,
                        "ts_min": int(chunk.get("ts_start_ms", 0) or 0),
                        "ts_max": int(chunk.get("ts_end_ms", 0) or 0),
                        "split_assignment": "auto_by_time",
                        "split_eligibility": "train_val_test",
                        "feature_schema_hash": hashlib.sha256(",".join(FEATURE_COLUMNS).encode("utf-8")).hexdigest(),
                        "schema_version": "market_shard_v1",
                        "feature_count": int(len(FEATURE_COLUMNS)),
                        "target_mode": str(config.get("target_mode", "triple_barrier")),
                        "label_schema": "runtime_default",
                        "size_mb": float(round(shard_size_mb, 4)),
                        "keep_training_shards": keep_training_shards,
                        "created_at": utc_now_iso(),
                    }
                    try:
                        manifest_path.parent.mkdir(parents=True, exist_ok=True)
                        with manifest_path.open("a", encoding="utf-8") as mf:
                            mf.write(json.dumps(shard_meta, separators=(",", ":"), ensure_ascii=True) + "\n")
                        await self._emit_run_log(
                            run_id,
                            "INFO",
                            (
                                f"Worker wrote shard path={out_path} rows={shard_row_count} windows=0 "
                                f"ts_min={shard_meta['ts_min']} ts_max={shard_meta['ts_max']} size_mb={shard_size_mb:.3f}"
                            ),
                        )
                        await self._emit_run_log(run_id, "INFO", "Parent received shard metadata only")
                        await self._emit_run_log(run_id, "INFO", f"Manifest row written path={manifest_path}")
                        rss_after_manifest = self._process_tree_resident_memory_bytes() or 0
                        await self._emit_run_log(
                            run_id,
                            "INFO",
                            f"Parent RSS after manifest write={rss_after_manifest / float(1024**3):.3f}GB",
                        )
                    except Exception as manifest_exc:
                        await self._emit_run_log(run_id, "WARNING", f"Manifest write failed: {manifest_exc}")
                    if out_path.exists():
                        with out_path.open("r", encoding="utf-8") as handle:
                            for line in handle:
                                line = line.strip()
                                if not line:
                                    continue
                                chunk_events.append(json.loads(line))
                        if keep_training_shards:
                            await self._emit_run_log(run_id, "INFO", f"Shard retained shard_id=shard_{chunk_index:06d}")
                        else:
                            try:
                                out_path.unlink()
                                await self._emit_run_log(run_id, "INFO", f"Shard deleted shard_id=shard_{chunk_index:06d}")
                            except Exception:
                                pass
                    events_after_hour_filter += int(len(chunk_events))
                    parent_rss_after_worker = self._process_tree_resident_memory_bytes() or 0
                    reclaimed_mb = max(0.0, (parent_rss_before_worker - parent_rss_after_worker) / float(1024**2))
                    await self._emit_run_log(
                        run_id,
                        "INFO",
                        f"Worker exited code={int(proc.exitcode or 0)}",
                    )
                    await self._emit_run_log(
                        run_id,
                        "INFO",
                        f"Parent RSS after worker exit={parent_rss_after_worker / float(1024**3):.3f}GB",
                    )
                    await self._emit_run_log(
                        run_id,
                        "INFO",
                        f"Process recycle memory reclaimed_mb={reclaimed_mb:.1f}",
                    )
                else:
                    pf = pq.ParquetFile(str(file_path))
                    for batch in pf.iter_batches(
                        columns=[
                            "source_exchange_id",
                            "stream",
                            "event_type",
                            "ts_exchange_ms",
                            "ts_receive_ms",
                            "payload_json",
                        ],
                        batch_size=25_000,
                    ):
                        cols = {
                            "source_exchange_id": batch.column(0).to_pylist(),
                            "stream": batch.column(1).to_pylist(),
                            "event_type": batch.column(2).to_pylist(),
                            "ts_exchange_ms": batch.column(3).to_pylist(),
                            "ts_receive_ms": batch.column(4).to_pylist(),
                            "payload_json": batch.column(5).to_pylist(),
                        }
                        row_count = len(cols["stream"])
                        events_scanned += int(row_count)
                        for idx in range(row_count):
                            source_exchange = str(cols["source_exchange_id"][idx] or "unknown").lower()
                            if selected_exchanges and source_exchange not in selected_exchanges:
                                continue
                            ts_receive_ms = int(cols["ts_receive_ms"][idx] or 0)
                            ts_exchange_ms = int(cols["ts_exchange_ms"][idx] or 0)
                            event_ts = ts_receive_ms if ts_receive_ms > 0 else ts_exchange_ms
                            if start_ts_ms is not None and event_ts < start_ts_ms:
                                continue
                            if end_ts_ms is not None and event_ts > end_ts_ms:
                                continue
                            events_after_hour_filter += 1
                            chunk_events.append(
                                {
                                    "source_exchange_id": source_exchange,
                                    "stream": str(cols["stream"][idx] or ""),
                                    "event_type": str(cols["event_type"][idx] or ""),
                                    "ts_receive_ms": ts_receive_ms,
                                    "ts_exchange_ms": ts_exchange_ms,
                                    "payload_json": str(cols["payload_json"][idx] or "{}"),
                                }
                            )
            except Exception as chunk_exc:
                await self._emit_run_log(run_id, "WARNING", f"Chunk extract failed ({file_path.name}): {chunk_exc}")
                continue

            approx_event_bytes = 2048
            memory_safety_factor = 1.35
            current_estimated_bytes = int((len(carry_events) + len(chunk_events)) * approx_event_bytes)
            if current_estimated_bytes > hard_guard_bytes:
                memory_pressure_stepdowns += 1
                drop_count = max(1, int(len(carry_events) * 0.35))
                carry_events = carry_events[drop_count:]
                await self._emit_run_log(
                    run_id,
                    "WARNING",
                    (
                        f"Memory pressure stepdown #{memory_pressure_stepdowns}: "
                        f"est_loaded_gb={current_estimated_bytes / float(1024**3):.3f} exceeded hard_guard_gb={hard_guard_bytes / float(1024**3):.3f}; "
                        f"carry_reduced_by={drop_count}"
                    ),
                )
                await self._runtime_boundary_cleanup(run_id=run_id, reason="stepdown_event", force=True)
                rss_tree_now = self._process_tree_resident_memory_bytes()
                await self._merge_run_metrics(
                    run_id,
                    {
                        "loaded_now_gb": float(round((rss_tree_now or 0) / float(1024**3), 6)),
                        "process_rss_gb": float(round((rss_tree_now or 0) / float(1024**3), 6)),
                    },
                    emit_update=True,
                )
                rss_now = self._process_tree_resident_memory_bytes()
                if rss_now is not None and rss_now >= strict_rss_cap_bytes:
                    await self._emit_run_log(
                        run_id,
                        "ERROR",
                        (
                            f"STRICT RAM cap still exceeded after stepdown: "
                            f"rss_gb={rss_now / float(1024**3):.3f} >= ram_budget_gb={strict_rss_cap_bytes / float(1024**3):.3f}. "
                            "Stopping to respect Training RAM Budget."
                        ),
                    )
                    await self._merge_run_metrics(
                        run_id,
                        {
                            "loaded_now_gb": float(round(rss_now / float(1024**3), 6)),
                            "process_rss_gb": float(round(rss_now / float(1024**3), 6)),
                            "ram_guard_triggered": True,
                        },
                        emit_update=True,
                    )
                    return None
            if not chunk_events and not carry_events and not group_events:
                processed_chunks += 1
                bytes_scanned += int(chunk.get("size_bytes", 0) or 0)
                continue

            group_events.extend(chunk_events)
            group_chunks += 1
            if raw_sample_seen < raw_sample_limit:
                for ev in chunk_events:
                    if raw_sample_seen >= raw_sample_limit:
                        break
                    raw_sample_seen += 1
                    for cand in raw_type_candidates:
                        val = str(ev.get(cand, "") or "").strip()
                        if not val:
                            continue
                        bucket = raw_type_unique_values[cand]
                        bucket[val] = int(bucket.get(val, 0) + 1)
                    parts = [
                        str(ev.get("event_type", "") or "").lower(),
                        str(ev.get("type", "") or "").lower(),
                        str(ev.get("e", "") or "").lower(),
                        str(ev.get("stream", "") or "").lower(),
                        str(ev.get("topic", "") or "").lower(),
                        str(ev.get("channel", "") or "").lower(),
                        str(ev.get("source", "") or "").lower(),
                    ]
                    joined = " ".join([p for p in parts if p])
                    ntype = "unknown"
                    if any(alias.lower() in joined for alias in ["bookticker", "book_ticker", "bookticker", "bbo", "ticker"]):
                        ntype = "bookticker"
                    elif any(alias.lower() in joined for alias in ["trade", "aggtrade", "agg_trade", "market_trade", "trade_batch"]):
                        ntype = "trade"
                    elif any(alias.lower() in joined for alias in ["depth", "depthupdate", "orderbook", "book_depth", "external_depth"]):
                        ntype = "depth"
                    elif "mark" in joined:
                        ntype = "mark"
                    elif "forceorder" in joined or "liquidation" in joined:
                        ntype = "liquidation"
                    normalized_event_type_counts[ntype] = int(normalized_event_type_counts.get(ntype, 0) + 1)
                    if ntype == "unknown":
                        key = joined[:120] if joined else "<empty>"
                        unmapped_event_type_counts[key] = int(unmapped_event_type_counts.get(key, 0) + 1)

            # RAM-targeted group flush: process only when we fill near target (or final chunk).
            group_estimated_bytes = int((len(carry_events) + len(group_events)) * approx_event_bytes * memory_safety_factor)
            # Budget-proportional aggressive fill:
            # larger configured RAM must materially increase in-flight group sizing.
            budget_gb = max(0.5, ram_budget_bytes / float(1024**3))
            cfg_chunk_group = config.get("training_chunk_group_size", "auto")
            parsed_chunk_group: int | None = None
            try:
                if isinstance(cfg_chunk_group, str) and cfg_chunk_group.strip().lower() == "auto":
                    parsed_chunk_group = None
                else:
                    parsed_chunk_group = int(cfg_chunk_group)  # type: ignore[arg-type]
            except Exception:
                parsed_chunk_group = None
            allowed_chunk_groups = {1, 2, 3, 5, 8, 10}
            if parsed_chunk_group is not None and parsed_chunk_group not in allowed_chunk_groups:
                parsed_chunk_group = None
            if runtime_forced_chunk_group_size is not None:
                parsed_chunk_group = int(runtime_forced_chunk_group_size)
            auto_chunks_per_group = int(min(800, max(48, round(64 * budget_gb))))
            recycle_enabled = bool(config.get("training_process_recycle_enabled", True))
            if parsed_chunk_group is None:
                max_chunks_per_group = auto_chunks_per_group
                if chunk_index == 1:
                    await self._emit_run_log(
                        run_id,
                        "INFO",
                        f"Selected chunk group size: Auto -> {max_chunks_per_group}",
                    )
            else:
                max_chunks_per_group = int(parsed_chunk_group)
                if chunk_index == 1:
                    await self._emit_run_log(
                        run_id,
                        "INFO",
                        f"Selected chunk group size: {max_chunks_per_group}",
                    )
            # Memory-safe default: when recycle mode is enabled, keep parent groups to 1 chunk.
            # This prevents parent-side accumulation across multiple large raw event chunks.
            if recycle_enabled and max_chunks_per_group > 1:
                max_chunks_per_group = 1
                if chunk_index == 1:
                    await self._emit_run_log(
                        run_id,
                        "INFO",
                        "Chunk-group safety override active: recycle_enabled -> max_chunks_per_group=1",
                    )
            max_events_per_group = int(min(8_000_000, max(300_000, round(420_000 * budget_gb))))
            max_group_seconds = float(min(45.0, max(6.0, 6.0 + (budget_gb * 1.8))))
            # Hybrid flush strategy:
            # 1) early small flush for fast first visible windows/heartbeat
            # 2) then aggressive RAM-targeted batching for throughput
            early_first_flush_bytes = int(max(256 * 1024 * 1024, min(target_bytes, int(1.0 * 1024**3))))
            early_first_flush_secs = 3.0
            early_first_flush_chunks = 12
            if not first_flush_done:
                should_flush_group = (
                    group_estimated_bytes >= early_first_flush_bytes
                    or group_chunks >= early_first_flush_chunks
                    or (time.monotonic() - group_started_at) >= early_first_flush_secs
                    or chunk_index == chunks_total
                )
            else:
                should_flush_group = (
                    group_estimated_bytes >= target_bytes
                    or chunk_index == chunks_total
                    or group_chunks >= max_chunks_per_group
                    or len(group_events) >= max_events_per_group
                    or (time.monotonic() - group_started_at) >= max_group_seconds
                )
            if should_flush_group:
                ram_before_load = self._process_tree_resident_memory_bytes() or 0
                await self._emit_run_log(
                    run_id,
                    "INFO",
                    (
                        f"Chunk group start: chunks={group_chunks} carry_events={len(carry_events)} group_events={len(group_events)} "
                        f"RAM before load={ram_before_load / float(1024**3):.3f}GB"
                    ),
                )
                combined_events = carry_events + group_events
                combined_events.sort(key=lambda item: (int(item.get("ts_receive_ms", 0) or 0), int(item.get("ts_exchange_ms", 0) or 0)))
                # True intra-group streaming conversion (micro-batches) to avoid monolithic RAM spikes.
                micro_batch_events = max(50_000, int(os.getenv("ML_GROUP_MICROBATCH_EVENTS", "150000")))
                overlap_events = max(5_000, int(os.getenv("ML_GROUP_MICROBATCH_OVERLAP_EVENTS", "25000")))
                shard_idx = len(feature_shard_entries) + 1
                feature_shard_path = feature_shard_dir / f"feature_rows_{shard_idx:06d}.jsonl"
                segment_limit_events = max(50_000, int(os.getenv("ML_GROUP_SEGMENT_EVENTS", "180000")))
                stream_stats: dict[str, Any] = {}
                stream_label_stats: dict[str, Any] = {}
                seg_start = 0
                seg_append = False
                seg_last_ts = int(last_feature_ts)
                agg_rows = 0
                agg_ts_min = 0
                agg_ts_max = 0
                agg_batches = 0
                agg_input_events = 0
                agg_dedup = 0
                while seg_start < len(combined_events):
                    seg_end = min(len(combined_events), seg_start + segment_limit_events)
                    segment_events = combined_events[seg_start:seg_end]
                    try:
                        _, seg_stats, seg_label_stats = self._market_events_to_feature_rows_streaming(
                            segment_events,
                            sample_ms_override=effective_sample_ms,
                            micro_batch_events=micro_batch_events,
                            overlap_events=overlap_events,
                            max_batch_seconds=int(os.getenv("ML_GROUP_MICROBATCH_TIMEOUT_S", "120")),
                            max_rss_gb=float(hard_guard_bytes / float(1024**3)),
                            output_path=feature_shard_path,
                            min_ts_exclusive=int(seg_last_ts),
                            append_output=seg_append,
                        )
                    except RuntimeError as seg_exc:
                        if "Feature conversion exceeded RAM guard" not in str(seg_exc):
                            raise
                        tighter_micro = max(25_000, micro_batch_events // 2)
                        tighter_overlap = max(2_500, overlap_events // 2)
                        _, seg_stats, seg_label_stats = self._market_events_to_feature_rows_streaming(
                            segment_events,
                            sample_ms_override=effective_sample_ms,
                            micro_batch_events=tighter_micro,
                            overlap_events=tighter_overlap,
                            max_batch_seconds=int(os.getenv("ML_GROUP_MICROBATCH_TIMEOUT_S", "120")),
                            max_rss_gb=float(hard_guard_bytes / float(1024**3)),
                            output_path=feature_shard_path,
                            min_ts_exclusive=int(seg_last_ts),
                            append_output=seg_append,
                        )
                    seg_rows = int(seg_stats.get("streaming_written_rows", 0) or 0)
                    seg_ts_min = int(seg_stats.get("streaming_written_ts_min", 0) or 0)
                    seg_ts_max = int(seg_stats.get("streaming_written_ts_max", 0) or 0)
                    agg_rows += seg_rows
                    agg_batches += int(seg_stats.get("streaming_batches_total", 0) or 0)
                    agg_input_events += int(seg_stats.get("streaming_input_events", 0) or 0)
                    agg_dedup += int(seg_stats.get("streaming_dedup_dropped", 0) or 0)
                    if agg_ts_min <= 0 and seg_ts_min > 0:
                        agg_ts_min = seg_ts_min
                    if seg_ts_max > 0:
                        agg_ts_max = seg_ts_max
                        seg_last_ts = seg_ts_max
                    if isinstance(seg_label_stats, dict):
                        stream_label_stats = seg_label_stats
                    seg_append = True
                    seg_start = seg_end
                    del segment_events
                stream_stats = {
                    "streaming_written_rows": int(agg_rows),
                    "streaming_written_ts_min": int(agg_ts_min),
                    "streaming_written_ts_max": int(agg_ts_max),
                    "streaming_batches_total": int(agg_batches),
                    "streaming_input_events": int(agg_input_events),
                    "streaming_dedup_dropped": int(agg_dedup),
                    "streaming_output_rows": int(agg_rows),
                }
                ram_after_load = self._process_tree_resident_memory_bytes() or 0
                emitted_rows = int((stream_stats or {}).get("streaming_written_rows", 0) or 0)
                group_windows = max(
                    0,
                    emitted_rows - int(config["training"]["lookback_steps"]) - int(max(config["horizons"])) + 1,
                )
                await self._emit_run_log(
                    run_id,
                    "INFO",
                    (
                        f"RAM after load={ram_after_load / float(1024**3):.3f}GB windows_in_group={group_windows}"
                    ),
                )
                if isinstance(stream_stats, dict):
                    await self._emit_run_log(
                        run_id,
                        "INFO",
                        (
                            f"Chunk group stats: batches={int(stream_stats.get('streaming_batches_total', 0))} "
                            f"input_events={int(stream_stats.get('streaming_input_events', 0))} "
                            f"output_rows={int(stream_stats.get('streaming_output_rows', 0))} "
                            f"dedup_dropped={int(stream_stats.get('streaming_dedup_dropped', 0))}"
                        ),
                    )
                kept_rows = int((stream_stats or {}).get("streaming_written_rows", 0) or 0)
                ts_min = int((stream_stats or {}).get("streaming_written_ts_min", 0) or 0)
                ts_max = int((stream_stats or {}).get("streaming_written_ts_max", 0) or 0)
                if kept_rows > 0 and ts_max > 0:
                    last_feature_ts = int(ts_max)
                    feature_rows_total += int(kept_rows)
                    win_count = int(
                        max(
                            0,
                            kept_rows - int(config["training"]["lookback_steps"]) - int(max(config["horizons"])) + 1,
                        )
                    )
                    feature_windows_total += int(win_count)
                    feature_shard_entries.append(
                        {
                            "shard_id": f"feature_rows_{shard_idx:06d}",
                            "shard_path": str(feature_shard_path),
                            "row_count": int(kept_rows),
                            "window_count": int(win_count),
                            "ts_min": int(ts_min),
                            "ts_max": int(ts_max),
                            "split_eligibility": "train_val_test",
                            "feature_schema_hash": hashlib.sha256(",".join(FEATURE_COLUMNS).encode("utf-8")).hexdigest(),
                            "schema_version": "feature_rows_v1",
                            "feature_count": int(len(FEATURE_COLUMNS)),
                            "target_mode": str(config.get("target_mode", "triple_barrier")),
                            "label_schema": "runtime_default",
                            "created_at": utc_now_iso(),
                        }
                    )
                    extraction_trace["feature_shards_written"] += 1
                    extraction_trace["feature_shard_rows_total"] += int(kept_rows)
                    extraction_trace["feature_manifest_rows"] += 1
                    await self._emit_run_log(
                        run_id,
                        "INFO",
                        (
                            f"Worker wrote shard path={feature_shard_path} rows={kept_rows} "
                            f"windows={win_count} ts_min={ts_min} ts_max={ts_max} "
                            f"size_mb={feature_shard_path.stat().st_size / float(1024**2):.3f}"
                        ),
                    )
                    await self._emit_run_log(run_id, "INFO", "Parent received shard metadata only")
                else:
                    try:
                        feature_shard_path.unlink(missing_ok=True)  # type: ignore[arg-type]
                    except Exception:
                        pass
                feature_stats = stream_stats if isinstance(stream_stats, dict) else getattr(self, "_last_market_feature_stats", None)
                if isinstance(feature_stats, dict):
                    for stat_key in aggregated_feature_stats:
                        aggregated_feature_stats[stat_key] += int(feature_stats.get(stat_key, 0) or 0)
                    for key in extraction_trace.keys():
                        if key in feature_stats:
                            extraction_trace[key] += int(feature_stats.get(key, 0) or 0)
                if isinstance(stream_label_stats, dict):
                    setattr(self, "_last_label_price_stats", stream_label_stats)

                newest_ts = 0
                for ev in reversed(combined_events):
                    newest_ts = int(ev.get("ts_receive_ms", 0) or ev.get("ts_exchange_ms", 0) or 0)
                    if newest_ts > 0:
                        break
                if newest_ts > 0:
                    threshold_ts = newest_ts - carry_ms
                    carry_events = [
                        ev for ev in combined_events if int(ev.get("ts_receive_ms", 0) or ev.get("ts_exchange_ms", 0) or 0) >= threshold_ts
                    ]
                else:
                    carry_events = combined_events[-5000:]
                group_events = []
                group_chunks = 0
                group_started_at = time.monotonic()
                first_flush_done = True
                # Drop large references before cleanup to help GC reclaim Python containers.
                try:
                    del chunk_events
                except Exception:
                    pass
                try:
                    del combined_events
                except Exception:
                    pass
                await self._runtime_boundary_cleanup(run_id=run_id, reason="chunk_group_release")
                ram_after_release = self._process_tree_resident_memory_bytes() or 0
                reclaimed_mb_group = max(0.0, (ram_before_load - ram_after_release) / float(1024**2))
                delta_mb_group = (ram_after_release - ram_before_load) / float(1024**2)
                await self._emit_run_log(
                    run_id,
                    "INFO",
                    f"Chunk group released: RAM after release={ram_after_release / float(1024**3):.3f}GB",
                )
                await self._emit_run_log(
                    run_id,
                    "INFO",
                    (
                        f"Parent RSS after release={ram_after_release / float(1024**3):.3f}GB "
                        f"reclaimed_mb={reclaimed_mb_group:.1f} parent_rss_delta_mb={delta_mb_group:.1f}"
                    ),
                )
                soft_limit = int(ram_budget_bytes * 0.95)
                if ram_after_release >= soft_limit:
                    ordered_sizes = [10, 8, 5, 3, 2, 1]
                    cur_size = int(parsed_chunk_group if parsed_chunk_group is not None else auto_chunks_per_group)
                    if cur_size not in ordered_sizes:
                        cur_size = next((v for v in ordered_sizes if v <= cur_size), 1)
                    idx = ordered_sizes.index(cur_size)
                    new_size = ordered_sizes[idx + 1] if (idx + 1) < len(ordered_sizes) else None
                    if new_size is not None:
                        runtime_forced_chunk_group_size = int(new_size)
                        await self._emit_run_log(
                            run_id,
                            "WARNING",
                            (
                                f"Auto stepdown chunk group size old={cur_size} new={new_size} "
                                f"reason=parent_rss_soft_limit"
                            ),
                        )
                    elif cur_size <= 1 and ram_after_release >= strict_rss_cap_bytes:
                        await self._emit_run_log(
                            run_id,
                            "ERROR",
                            "memory_hard_guard_exceeded_in_shard_mode",
                        )
                        return None

            processed_chunks += 1
            bytes_scanned += int(chunk.get("size_bytes", 0) or 0)
            last_progress_monotonic = time.monotonic()
            if (chunk_index == 1) or (chunk_index % 5 == 0) or (chunk_index == chunks_total):
                scanned_gb = float(bytes_scanned / float(1024**3))
                provisional_windows_total = int(max(0, feature_windows_total))
                rss_tree_now = self._process_tree_resident_memory_bytes()
                await self._merge_run_metrics(
                    run_id,
                    {
                        "chunks_processed": int(processed_chunks),
                        "data_chunks_processed": int(processed_chunks),
                        "used_data_gb": float(round(scanned_gb, 6)),
                        "loaded_now_gb": float(round((rss_tree_now or 0) / float(1024**3), 6)),
                        "process_rss_gb": float(round((rss_tree_now or 0) / float(1024**3), 6)),
                        "windows_total": int(max(0, provisional_windows_total)),
                    },
                    emit_update=True,
                )
                await self._emit_run_log(
                    run_id,
                    "INFO",
                    (
                        f"Feature build heartbeat: chunks={processed_chunks}/{chunks_total} "
                        f"group_events={len(group_events)} carry_events={len(carry_events)} "
                        f"loaded_gb={((len(carry_events)+len(group_events))*approx_event_bytes*memory_safety_factor)/(1024**3):.3f}"
                    ),
                )
                await self._emit_run_log(
                    run_id,
                    "INFO",
                    (
                        f"Feature proof progress: chunks={processed_chunks}/{chunks_total} "
                        f"shards_written={int(extraction_trace.get('feature_shards_written', 0))} "
                        f"shard_rows={int(extraction_trace.get('feature_shard_rows_total', 0))}"
                    ),
                )

        await self._emit_run_log(run_id, "INFO", "Chunk streaming complete")
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                f"Feature shard phase complete: shards={int(extraction_trace.get('feature_shards_written', 0))} "
                f"rows={int(extraction_trace.get('feature_shard_rows_total', 0))} manifest={manifest_path}"
            ),
        )
        extraction_trace["feature_manifest_paths_count"] = int(len(feature_shard_entries))
        extraction_trace["feature_rows_emitted"] = int(aggregated_feature_stats.get("rows_emitted", 0) or 0)
        extraction_trace["windows_attempted"] = int(extraction_trace.get("grid_rows_attempted", 0))
        await self._merge_run_metrics(run_id, {"feature_extraction_trace": extraction_trace}, emit_update=True)
        if (
            int(extraction_trace.get("scanned_events", 0)) > 0
            and int(extraction_trace.get("feature_rows_emitted", 0)) == 0
            and int(extraction_trace.get("feature_shard_rows_total", 0)) <= 0
        ):
            reject_summary = {
                "missing_price": int(extraction_trace.get("rows_rejected_missing_price", 0)),
                "stale_book": int(extraction_trace.get("rows_rejected_stale_book", 0)),
                "stale_trade": int(extraction_trace.get("rows_rejected_stale_trade", 0)),
                "spread": int(extraction_trace.get("rows_rejected_spread", 0)),
                "missing_features": int(extraction_trace.get("rows_rejected_missing_features", 0)),
            }
            zero_root_cause = "feature_rows_not_counted_or_not_manifested"
            if int(extraction_trace.get("feature_shards_written", 0)) <= 0 and int(extraction_trace.get("feature_manifest_rows", 0)) <= 0:
                zero_root_cause = "feature_shard_manifest_missing"
            elif int(extraction_trace.get("feature_shard_rows_total", 0)) > 0 and int(extraction_trace.get("feature_rows_emitted", 0)) <= 0:
                zero_root_cause = "feature_rows_written_but_not_registered"
            await self._emit_run_log(run_id, "ERROR", f"feature_extraction_trace={json.dumps(extraction_trace, separators=(',', ':'), ensure_ascii=True)}")
            await self._emit_run_log(run_id, "ERROR", f"feature_extraction_failed_zero_rows top_reject_reasons={reject_summary} zero_root_cause={zero_root_cause}")
            fail_report = self._build_data_health_report(
                chunk_rows=chunk_rows,
                events_scanned=events_scanned,
                events_after_hour_filter=events_after_hour_filter,
                aggregated_feature_stats=aggregated_feature_stats,
                dataset=None,
                config=config,
            )
            fail_reasons = list(fail_report.get("fail_reasons", []))
            if "feature_extraction_failed_zero_rows" not in fail_reasons:
                fail_reasons.append("feature_extraction_failed_zero_rows")
            fail_report["fail_reasons"] = fail_reasons
            fail_report["data_health_ok"] = False
            fail_report["feature_extraction_trace"] = extraction_trace
            fail_report["zero_row_root_cause"] = zero_root_cause
            fail_report["data_health_summary"] = (
                str(fail_report.get("data_health_summary", "")) + f" zero_row_root_cause={zero_root_cause}"
            ).strip()
            await self._write_data_health_report(run_id, fail_report)
            run_ref = await self._repository.get_run(run_id)
            run_dir = self._canonical_run_dir_from_run(run_ref or {"run_id": run_id, "pair_symbol": pair_symbol, "instance_id": config.get("instance_id", ""), "config": config})
            await self._emit_run_log(run_id, "INFO", f"Canonical run_dir={run_dir}")
            await self._emit_run_log(run_id, "INFO", f"Data health report written: {run_dir / 'data_health_report.json'}")
            await self._emit_run_log(run_id, "INFO", f"data_health_ok=False fail_reasons={fail_report.get('fail_reasons', [])}")
            await self._merge_run_metrics(
                run_id,
                {
                    "feature_extraction_trace": extraction_trace,
                    "data_health_ok": False,
                    "data_health_fail_reasons": list(fail_report.get("fail_reasons", [])),
                    "data_health_summary": str(fail_report.get("data_health_summary", "")),
                    "recommended_live_mode": "HOLD_ONLY",
                    "model_result_judgeable": False,
                    "model_result_block_reason": "data_health_failure",
                },
                emit_update=True,
            )
            return None

        if not feature_shard_entries:
            await self._emit_run_log(run_id, "WARNING", f"No market events found for {pair_symbol} after filtering.")
            return None
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                "Planner coverage: "
                f"events_scanned={events_scanned} "
                f"events_after_hour_filter={events_after_hour_filter} "
                f"hour_window_enabled={hour_window_enabled}"
            ),
        )
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                f"Market events prepared for feature build: events={events_after_hour_filter} "
                f"(cap={'unlimited' if max_events_env <= 0 else max_events_env})"
            ),
        )
        if max_events_env > 0 and feature_rows_total > max_events_env:
            await self._emit_run_log(
                run_id,
                "WARNING",
                "ML_MARKET_EVENTS_MAX_EVENTS trimming is disabled in shard mode; keeping produced shards as-is.",
            )
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                "Market-feature stats: "
                f"events_total={int(aggregated_feature_stats.get('events_total', 0))} "
                f"tool_filtered={int(aggregated_feature_stats.get('events_tool_filtered', 0))} "
                f"json_error={int(aggregated_feature_stats.get('events_json_error', 0))} "
                f"bookticker={int(aggregated_feature_stats.get('events_bookticker', 0))} "
                f"depth={int(aggregated_feature_stats.get('events_depth', 0))} "
                f"trade={int(aggregated_feature_stats.get('events_trade', 0))} "
                f"mark={int(aggregated_feature_stats.get('events_mark', 0))} "
                f"liq={int(aggregated_feature_stats.get('events_liq', 0))} "
                f"skip_no_mid={int(aggregated_feature_stats.get('skip_no_mid', 0))} "
                f"skip_sample_gate={int(aggregated_feature_stats.get('skip_sample_gate', 0))} "
                f"rows_emitted={int(aggregated_feature_stats.get('rows_emitted', 0))}"
            ),
        )
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                "Feature-row build complete: "
                f"rows={feature_rows_total} sample_ms={effective_sample_ms}"
            ),
        )
        await self._merge_run_metrics(
            run_id,
            {
                "chunks_processed": int(processed_chunks),
                "data_chunks_processed": int(processed_chunks),
                "used_data_gb": float(round(total_data_gb, 6)),
                "loaded_now_gb": 0.0,
            },
            emit_update=True,
        )
        await self._emit_run_log(run_id, "INFO", "Shard mode dataset builder active")
        shard_config = dict(config)
        shard_config["_shard_manifest_path"] = str(manifest_path)
        shard_config["_shard_tmp_root"] = str(chunk_worker_tmp_root)
        shard_config["_use_chunk_worker"] = bool(use_chunk_worker)
        dataset = await self._build_dataset_from_feature_shards(feature_shard_entries, shard_config, run_id)
        extraction_trace["feature_rows_registered_for_dataset"] = int(sum(int(x.get("row_count", 0) or 0) for x in feature_shard_entries))
        window_trace = (dataset or {}).get("window_assembly_trace", {}) if isinstance(dataset, dict) else {}
        extraction_trace["windows_attempted"] = int(window_trace.get("windows_attempted", 0) or 0)
        extraction_trace["windows_final"] = int(window_trace.get("windows_final", int((dataset or {}).get("windows_total", 0) or 0)) or 0)
        await self._emit_run_log(
            run_id,
            "INFO",
            f"Window assembly from shards: attempted={int(extraction_trace.get('windows_attempted', 0))} final={int(extraction_trace.get('windows_final', 0))}",
        )
        if int(extraction_trace.get("feature_rows_registered_for_dataset", 0)) > 0 and int(extraction_trace.get("windows_attempted", 0)) == 0:
            zero_reason = str(window_trace.get("zero_window_root_cause", "") or "").strip()
            if not zero_reason:
                if not isinstance(dataset, dict) or not dataset:
                    zero_reason = "window_builder_not_called"
                else:
                    zero_reason = "shard_iterator_not_passed_to_window_builder"
            await self._emit_run_log(
                run_id,
                "ERROR",
                f"window_assembly_failed_zero_attempts reason={zero_reason}",
            )
            await self._merge_run_metrics(
                run_id,
                {
                    "window_assembly_trace": dict(window_trace) if isinstance(window_trace, dict) else {},
                    "zero_window_root_cause": zero_reason,
                    "feature_extraction_trace": extraction_trace,
                    "data_health_ok": False,
                    "data_health_fail_reasons": ["window_assembly_failed_zero_attempts"],
                    "data_health_summary": f"registered_rows={extraction_trace.get('feature_rows_registered_for_dataset', 0)} windows_attempted=0 reason={zero_reason}",
                },
                emit_update=True,
            )
            return None
        if use_chunk_worker and not keep_training_shards:
            try:
                shutil.rmtree(chunk_worker_tmp_root, ignore_errors=True)
            except Exception:
                pass
        dataset_for_health = dict(dataset or {})
        dataset_for_health["feature_extraction_trace"] = extraction_trace
        dataset_for_health["raw_event_type_audit"] = {
            "raw_event_type_candidates": {k: int(sum(v.values())) for k, v in raw_type_unique_values.items()},
            "raw_event_type_unique_values": {k: dict(sorted(v.items(), key=lambda item: item[1], reverse=True)[:30]) for k, v in raw_type_unique_values.items()},
            "normalized_event_type_counts": normalized_event_type_counts,
            "event_type_mapping_used": event_type_mapping_used,
            "unmapped_event_type_counts": dict(sorted(unmapped_event_type_counts.items(), key=lambda item: item[1], reverse=True)[:30]),
        }
        report = self._build_data_health_report(
            chunk_rows=chunk_rows,
            events_scanned=events_scanned,
            events_after_hour_filter=events_after_hour_filter,
            aggregated_feature_stats=aggregated_feature_stats,
            dataset=dataset_for_health,
            config=config,
        )
        report["feature_extraction_trace"] = extraction_trace
        await self._write_data_health_report(run_id, report)
        run_ref = await self._repository.get_run(run_id)
        run_dir = self._canonical_run_dir_from_run(run_ref or {"run_id": run_id, "pair_symbol": pair_symbol, "instance_id": config.get("instance_id", ""), "config": config})
        await self._emit_run_log(run_id, "INFO", f"Canonical run_dir={run_dir}")
        await self._emit_run_log(run_id, "INFO", f"Data health report written: {run_dir / 'data_health_report.json'}")
        await self._emit_run_log(
            run_id,
            "INFO",
            f"data_health_ok={bool(report.get('data_health_ok', False))} fail_reasons={report.get('fail_reasons', [])}",
        )
        await self._merge_run_metrics(
            run_id,
            {
                "feature_extraction_trace": extraction_trace,
                "data_health_ok": bool(report.get("data_health_ok", False)),
                "data_health_fail_reasons": list(report.get("fail_reasons", [])),
                "data_health_summary": str(report.get("data_health_summary", "")),
                "label_probe_summary": dict(report.get("label_probe_summary", {}) or {}),
            },
            emit_update=True,
        )
        training_epochs = int(((config.get("training", {}) or {}).get("epochs", 1)) or 0)
        if bool(config.get("audit_only", False)) or training_epochs <= 0:
            await self._emit_run_log(run_id, "INFO", "Audit-only mode active: stopping after data health export.")
            await self._merge_run_metrics(
                run_id,
                {
                    "audit_only": True,
                    "model_result_judgeable": False,
                    "model_result_block_reason": "audit_only_export",
                    "recommended_live_mode": "HOLD_ONLY",
                },
                emit_update=True,
            )
            return None
        if not bool(report.get("data_health_ok", False)):
            await self._merge_run_metrics(
                run_id,
                {
                    "recommended_live_mode": "HOLD_ONLY",
                    "model_result_judgeable": False,
                    "model_result_block_reason": "data_health_failure",
                },
                emit_update=True,
            )
            await self._emit_run_log(run_id, "ERROR", "Model quality judgment blocked: data_health_ok=false")
            return None
        return dataset

    async def _build_dataset_from_feature_shards(
        self,
        shard_entries: list[dict[str, Any]],
        config: dict[str, Any],
        run_id: str,
    ) -> dict[str, Any] | None:
        if not shard_entries:
            await self._emit_run_log(run_id, "WARNING", "No feature shards produced in shard mode.")
            return None
        keep_training_shards = bool(config.get("keep_training_shards", False))
        await self._emit_run_log(run_id, "INFO", "No global row accumulation path used")
        ram_budget_bytes = self._training_memory_budget_bytes(config=config)
        soft_limit = int(ram_budget_bytes * 0.95)
        hard_limit = int(max(96 * 1024 * 1024, ram_budget_bytes * 0.98))
        dataset_source_mode = str(config.get("dataset_source", "features_manifest") or "features_manifest").strip().lower()
        if dataset_source_mode == "local_market_events":
            manifest_path = Path(str(config.get("_shard_manifest_path", "") or "")).resolve()
            return await self._build_dataset_from_manifest_streaming(
                manifest_path=manifest_path,
                shard_entries=shard_entries,
                config=config,
                run_id=run_id,
            )
        rows: list[dict[str, Any]] = []
        for entry in shard_entries:
            shard_id = str(entry.get("shard_id", "unknown"))
            shard_path = Path(str(entry.get("shard_path", "")))
            if not shard_path.exists():
                continue
            parent_rss_before_group = self._process_tree_resident_memory_bytes() or 0
            await self._emit_run_log(
                run_id,
                "INFO",
                f"Parent RSS before group={parent_rss_before_group / float(1024**3):.3f}GB",
            )
            await self._emit_run_log(run_id, "INFO", f"Streaming shard start shard_id={shard_id} split=auto")
            shard_rows = 0
            with shard_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    rows.append(self._compact_feature_row(json.loads(line)))
                    shard_rows += 1
            await self._emit_run_log(
                run_id,
                "INFO",
                f"Streaming shard end shard_id={shard_id} rows={shard_rows} windows=0",
            )
            rss_after_stream = self._process_tree_resident_memory_bytes() or 0
            await self._emit_run_log(
                run_id,
                "INFO",
                f"Parent RSS after shard consumed={rss_after_stream / float(1024**3):.3f}GB",
            )
            await self._emit_run_log(run_id, "INFO", f"Shard released shard_id={shard_id}")
            # Proof pass must not mutate shard files before dataset assembly consumes them.
            # Deletion/retention is handled after actual training consumption.
            if keep_training_shards:
                await self._emit_run_log(run_id, "INFO", f"Shard retained shard_id={shard_id}")
            else:
                await self._emit_run_log(run_id, "INFO", f"Shard marked_for_delete shard_id={shard_id}")
            rss_after_release = self._process_tree_resident_memory_bytes() or 0
            reclaimed_mb = max(0.0, (parent_rss_before_group - rss_after_release) / float(1024**2))
            delta_mb = (rss_after_release - parent_rss_before_group) / float(1024**2)
            await self._emit_run_log(
                run_id,
                "INFO",
                (
                    f"Parent RSS after shard release={rss_after_release / float(1024**3):.3f}GB "
                    f"reclaimed_mb={reclaimed_mb:.1f} parent_rss_delta_mb={delta_mb:.1f}"
                ),
            )
            if rss_after_release >= soft_limit:
                cur_cfg = config.get("training_chunk_group_size", "auto")
                try:
                    cur_size = int(cur_cfg)
                except Exception:
                    cur_size = 10
                ordered_sizes = [10, 8, 5, 3, 2, 1]
                if cur_size not in ordered_sizes:
                    cur_size = next((v for v in ordered_sizes if v <= cur_size), 1)
                idx = ordered_sizes.index(cur_size)
                next_size = ordered_sizes[idx + 1] if (idx + 1) < len(ordered_sizes) else 1
                config["training_chunk_group_size"] = int(next_size)
                await self._emit_run_log(
                    run_id,
                    "WARNING",
                    f"Auto stepdown chunk group size old={cur_size} new={next_size} reason=parent_rss_soft_limit",
                )
            if rss_after_release >= hard_limit:
                await self._emit_run_log(run_id, "ERROR", "memory_hard_guard_exceeded_in_shard_mode")
                return None
        return await self._build_dataset_from_feature_rows(rows, config, run_id)

    async def _build_dataset_from_manifest_streaming(
        self,
        *,
        manifest_path: Path,
        shard_entries: list[dict[str, Any]],
        config: dict[str, Any],
        run_id: str,
    ) -> dict[str, Any] | None:
        await self._emit_run_log(run_id, "INFO", "No global row accumulation path used")
        await self._emit_run_log(run_id, "INFO", "Shard mode dataset builder active")
        if not manifest_path.exists():
            await self._emit_run_log(run_id, "ERROR", f"Manifest missing: {manifest_path}")
            return None
        keep_training_shards = bool(config.get("keep_training_shards", False))
        manifest_rows: list[dict[str, Any]] = []
        try:
            with manifest_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(payload, dict):
                        manifest_rows.append(payload)
        except Exception as exc:
            await self._emit_run_log(run_id, "ERROR", f"Manifest read failed: {exc}")
            return None
        if not manifest_rows:
            await self._emit_run_log(run_id, "ERROR", "Manifest is empty; no shard metadata available.")
            return None

        await self._emit_run_log(
            run_id,
            "INFO",
            f"Manifest rows loaded: count={len(manifest_rows)} path={manifest_path}",
        )
        feature_manifest_rows = [
            row
            for row in manifest_rows
            if str(row.get("schema_version", "")).strip().lower().startswith("feature_rows")
        ]
        if not feature_manifest_rows:
            # Backward-safe fallback: use in-memory shard metadata collected during this run.
            feature_manifest_rows = [dict(item) for item in shard_entries if isinstance(item, dict)]
        if not feature_manifest_rows:
            await self._emit_run_log(
                run_id,
                "ERROR",
                "Manifest has no feature-row shard entries and no runtime shard metadata fallback.",
            )
            return None
        await self._emit_run_log(
            run_id,
            "INFO",
            f"Feature manifest rows selected: count={len(feature_manifest_rows)}",
        )
        manifest_row_total = int(sum(int(row.get("row_count", 0) or 0) for row in feature_manifest_rows))
        await self._emit_run_log(
            run_id,
            "INFO",
            f"Feature manifest consumed: paths={len(feature_manifest_rows)} rows={manifest_row_total}",
        )
        # Log-only proof pass over 5 groups (or fewer if short run) without materializing global rows.
        proof_groups = 0
        for entry in feature_manifest_rows:
            shard_id = str(entry.get("shard_id", "unknown"))
            shard_path = Path(str(entry.get("shard_path", "") or ""))
            if not shard_path.exists():
                continue
            rss_before = self._process_tree_resident_memory_bytes() or 0
            await self._emit_run_log(run_id, "INFO", f"Parent RSS before group={rss_before / float(1024**3):.3f}GB")
            await self._emit_run_log(run_id, "INFO", f"Streaming shard start shard_id={shard_id} split=auto")
            row_count = 0
            try:
                with shard_path.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        if line.strip():
                            row_count += 1
            except Exception:
                continue
            await self._emit_run_log(run_id, "INFO", f"Streaming shard end shard_id={shard_id} rows={row_count} windows=0")
            rss_after_stream = self._process_tree_resident_memory_bytes() or 0
            await self._emit_run_log(run_id, "INFO", f"Parent RSS after shard consumed={rss_after_stream / float(1024**3):.3f}GB")
            await self._emit_run_log(run_id, "INFO", f"Shard released shard_id={shard_id}")
            if keep_training_shards:
                await self._emit_run_log(run_id, "INFO", f"Shard retained shard_id={shard_id}")
            else:
                await self._emit_run_log(run_id, "INFO", f"Shard marked_for_delete shard_id={shard_id}")
            await self._runtime_boundary_cleanup(run_id=run_id, reason="shard_manifest_release")
            rss_after_release = self._process_tree_resident_memory_bytes() or 0
            reclaimed_mb = max(0.0, (rss_before - rss_after_release) / float(1024**2))
            delta_mb = (rss_after_release - rss_before) / float(1024**2)
            await self._emit_run_log(
                run_id,
                "INFO",
                (
                    f"Parent RSS after shard release={rss_after_release / float(1024**3):.3f}GB "
                    f"reclaimed_mb={reclaimed_mb:.1f} parent_rss_delta_mb={delta_mb:.1f}"
                ),
            )
            proof_groups += 1
            if proof_groups >= 5:
                break

        # Build dataset via manifest order. This avoids the old direct shard-list path and
        # keeps all reads bound to shard files referenced by the manifest.
        rows: list[dict[str, Any]] = []
        for entry in feature_manifest_rows:
            shard_path = Path(str(entry.get("shard_path", "") or ""))
            if not shard_path.exists():
                continue
            with shard_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(payload, dict):
                        rows.append(self._compact_feature_row(payload))
        await self._emit_run_log(run_id, "INFO", f"Dataset rows registered from shards: rows={len(rows)}")
        if manifest_row_total > 0 and len(rows) <= 0:
            first_paths = [str(Path(str(r.get("shard_path", "") or ""))) for r in feature_manifest_rows[:3]]
            await self._emit_run_log(
                run_id,
                "ERROR",
                f"feature_manifest_registration_failed manifest_path={manifest_path} sample_shards={first_paths}",
            )
            await self._merge_run_metrics(
                run_id,
                {
                    "feature_extraction_trace": {
                        "feature_manifest_rows": int(len(feature_manifest_rows)),
                        "feature_manifest_paths_count": int(len(feature_manifest_rows)),
                        "feature_shard_rows_total": int(manifest_row_total),
                        "feature_rows_registered_for_dataset": 0,
                        "windows_attempted": 0,
                        "windows_final": 0,
                    },
                    "data_health_ok": False,
                    "data_health_fail_reasons": ["feature_manifest_registration_failed"],
                    "data_health_summary": f"manifest_rows={manifest_row_total} registered_rows=0 manifest_path={manifest_path}",
                    "zero_row_root_cause": "feature_manifest_registration_failed",
                },
                emit_update=True,
            )
            return None
        shard_config = dict(config)
        shard_config["_shard_mode_builder"] = True
        return await self._build_dataset_from_feature_rows(rows, shard_config, run_id)

    async def _build_dataset_from_feature_rows(
        self,
        rows: list[dict[str, Any]],
        config: dict[str, Any],
        run_id: str,
    ) -> dict[str, Any] | None:
        window_trace: dict[str, Any] = {
            "registered_rows_total": int(len(rows)),
            "sorted_rows_total": 0,
            "usable_rows_after_timestamp_sort": 0,
            "duplicate_timestamp_rows_dropped": 0,
            "rows_rejected_missing_label_price": 0,
            "rows_rejected_bad_timestamp": 0,
            "rows_rejected_split_boundary": 0,
            "rows_rejected_lookback": 0,
            "rows_rejected_horizon": 0,
            "rows_rejected_stale_window": 0,
            "windows_attempted": 0,
            "windows_final": 0,
        }
        source_name = "feature_shards" if bool(config.get("_shard_mode_builder", False)) else "legacy_feature_rows"
        await self._emit_run_log(run_id, "INFO", f"Window builder entered: source={source_name} input_rows={len(rows)}")
        await self._emit_run_log(run_id, "INFO", f"Window builder input rows: rows={len(rows)} source={source_name}")
        dataset_source_mode = str(config.get("dataset_source", "features_manifest") or "features_manifest").strip().lower()
        if dataset_source_mode == "local_market_events" and not bool(config.get("_shard_mode_builder", False)):
            raise RuntimeError("Long dataset mode requires shard-manifest streaming; full feature_rows path disabled")
        if len(rows) < config["training"]["lookback_steps"] + max(config["horizons"]) + 10:
            await self._emit_run_log(run_id, "WARNING", f"Not enough rows after filtering: {len(rows)}")
            return None
        rows.sort(key=lambda item: int(item.get("ts_ms", 0)))
        window_trace["sorted_rows_total"] = int(len(rows))
        ts_vals = [int(item.get("ts_ms", 0) or 0) for item in rows]
        bad_ts = sum(1 for t in ts_vals if t <= 0)
        dup_ts = max(0, len(ts_vals) - len(set(ts_vals)))
        window_trace["rows_rejected_bad_timestamp"] = int(bad_ts)
        window_trace["duplicate_timestamp_rows_dropped"] = int(dup_ts)
        window_trace["usable_rows_after_timestamp_sort"] = int(max(0, len(rows) - bad_ts))
        disabled_columns: set[str] = set()
        for tool_id, columns in FEATURE_COLUMNS_BY_TOOL.items():
            if not self._mode_tool_enabled("training", tool_id):
                disabled_columns.update(columns)
        active_feature_columns = [col for col in FEATURE_COLUMNS if col not in disabled_columns]
        if not active_feature_columns:
            await self._emit_run_log(run_id, "WARNING", "All training feature groups are disabled by Manage Tools matrix.")
            return None
        feature_matrix = np.array(
            [
                [float(row.get(col, 0.0) or 0.0) for col in FEATURE_COLUMNS]
                for row in rows
            ],
            dtype=np.float32,
        )
        if disabled_columns:
            for col_idx, col_name in enumerate(FEATURE_COLUMNS):
                if col_name in disabled_columns:
                    feature_matrix[:, col_idx] = 0.0
        prices = np.array([float(row.get("label_price", 0.0) or 0.0) for row in rows], dtype=np.float32)
        valid_mask = prices > 0
        window_trace["rows_rejected_missing_label_price"] = int(len(rows) - int(valid_mask.sum()))
        if valid_mask.sum() < config["training"]["lookback_steps"] + max(config["horizons"]) + 10:
            await self._emit_run_log(run_id, "WARNING", "Not enough valid label-price rows after filtering.")
            await self._merge_run_metrics(
                run_id,
                {"window_assembly_trace": window_trace, "zero_window_root_cause": "label_price_series_missing"},
                emit_update=True,
            )
            return None
        anomaly_count = 0
        anomaly_samples: list[dict[str, Any]] = []
        for i in range(1, len(prices)):
            prev = float(prices[i - 1])
            cur = float(prices[i])
            if prev <= 0 or cur <= 0:
                continue
            jump = abs((cur / prev) - 1.0)
            if jump > 0.01:
                anomaly_count += 1
                if len(anomaly_samples) < 20:
                    anomaly_samples.append(
                        {
                            "index": int(i),
                            "ts_prev_ms": int(rows[i - 1].get("ts_ms", 0) or 0),
                            "ts_ms": int(rows[i].get("ts_ms", 0) or 0),
                            "prev_price": float(round(prev, 8)),
                            "price": float(round(cur, 8)),
                            "jump_pct": float(round(jump * 100.0, 6)),
                        }
                    )
                await self._emit_run_log(
                    run_id,
                    "WARNING",
                    f"price_jump_anomaly idx={i} ts_prev={int(rows[i - 1].get('ts_ms', 0) or 0)} ts={int(rows[i].get('ts_ms', 0) or 0)} prev={prev:.8f} cur={cur:.8f} jump_pct={jump*100.0:.4f}",
                )
        label_price_stats = getattr(self, "_last_label_price_stats", {}) if isinstance(getattr(self, "_last_label_price_stats", {}), dict) else {}
        label_price_ffill_rate = float(label_price_stats.get("ffill_rate", 0.0) or 0.0)
        bookticker_updates_total = int(label_price_stats.get("bookticker_updates_total", 0) or 0)
        label_price_rejected_count = int(label_price_stats.get("rejected_count", 0) or 0)
        label_price_rejected_rate = float(label_price_stats.get("rejected_rate", 0.0) or 0.0)
        label_price_rejection_samples = list(label_price_stats.get("rejection_samples", []) or [])
        max_label_price_rejected_rate = float(
            (
                (config.get("paper_bot", {}) if isinstance(config.get("paper_bot", {}), dict) else {}).get(
                    "max_label_price_rejected_rate",
                    0.005,
                )
            )
            or 0.005
        )
        label_price_quality_guard_triggered = bool(label_price_rejected_rate > max_label_price_rejected_rate)
        fail_on_label_price_quality_guard = bool(config.get("fail_on_label_price_quality_guard", False))
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                "Label price source: bookticker_mid_state "
                f"(ffill_rate={label_price_ffill_rate:.4f}, "
                f"direct_count={int(label_price_stats.get('direct_count', 0) or 0)}, "
                f"ffill_count={int(label_price_stats.get('ffill_count', 0) or 0)}, "
                f"bookticker_updates_total={bookticker_updates_total}, "
                f"rejected_count={label_price_rejected_count}, "
                f"rejected_rate={label_price_rejected_rate*100.0:.4f}%)"
            ),
        )
        if label_price_quality_guard_triggered:
            await self._merge_run_metrics(
                run_id,
                {
                    "label_price_source_used": "bookticker_mid_state",
                    "bookticker_updates_total": int(bookticker_updates_total),
                    "label_price_rejected_count": int(label_price_rejected_count),
                    "label_price_rejected_rate": float(label_price_rejected_rate),
                    "max_label_price_rejected_rate_used": float(max_label_price_rejected_rate),
                    "label_price_quality_guard_triggered": True,
                    "label_price_anomaly_samples": label_price_rejection_samples[:20],
                },
                emit_update=True,
            )
            guard_msg = (
                "Label-price quality guard triggered before training: "
                f"rejected={label_price_rejected_count} "
                f"bookticker_updates_total={bookticker_updates_total} "
                f"rejected_rate={label_price_rejected_rate*100.0:.4f}% "
                f"threshold={max_label_price_rejected_rate*100.0:.4f}%"
            )
            if fail_on_label_price_quality_guard:
                await self._mark_failed(run_id, guard_msg)
                return None
            await self._emit_run_log(
                run_id,
                "WARNING",
                guard_msg + " (continuing because fail_on_label_price_quality_guard=false)",
            )
        rolling_window = int(config["normalizer_window"])
        normalized = self._rolling_normalize(feature_matrix, rolling_window)
        lookback = int(config["training"]["lookback_steps"])
        horizon = int(max(config["horizons"]))
        threshold = float(config["label_threshold_pct"]) / 100.0
        target_mode = str(config.get("target_mode", "triple_barrier") or "triple_barrier").strip().lower()
        tb_cfg = config.get("triple_barrier", {}) if isinstance(config.get("triple_barrier", {}), dict) else {}
        tb_tp = float(tb_cfg.get("tp_pct", 0.08)) / 100.0
        tb_sl = float(tb_cfg.get("sl_pct", 0.05)) / 100.0
        tb_timeout = int(tb_cfg.get("timeout_steps", horizon))
        valid_ts = np.asarray([int(r.get("ts_ms", 0) or 0) for r in rows if int(r.get("ts_ms", 0) or 0) > 0], dtype=np.int64)
        if valid_ts.size > 1:
            ts_deltas = np.diff(valid_ts)
            median_step_ms = int(max(1, np.median(ts_deltas)))
        else:
            median_step_ms = 100
        timeout_ms_cfg = int(tb_cfg.get("timeout_ms", 0) or 0)
        stale_rows_total = int(sum(1 for r in rows if bool(r.get("is_stale_row", False))))
        stale_rows_rate = float(stale_rows_total / max(1, len(rows)))
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                f"Strict grid enabled: interval_ms={median_step_ms} "
                f"target_sample_ms={max(100, int(config.get('_effective_sample_ms', 100) or 100))} "
                f"stale_rows={stale_rows_total}/{len(rows)} ({stale_rows_rate*100.0:.2f}%)"
            ),
        )
        paper_cfg = config.get("paper_bot", {}) if isinstance(config.get("paper_bot"), dict) else {}
        max_hold_ms_cfg = int(paper_cfg.get("max_hold_ms", 0) or 0)
        if max_hold_ms_cfg <= 0:
            max_hold_ms_cfg = int(float(paper_cfg.get("max_hold_seconds", 0) or 0) * 1000.0)
        timeout_ms = timeout_ms_cfg if timeout_ms_cfg > 0 else int(tb_timeout * median_step_ms)
        if target_mode == "trade_outcome" and max_hold_ms_cfg > 0:
            timeout_ms = int(max_hold_ms_cfg)
        timeout_steps_effective = max(1, int(round(timeout_ms / max(1, median_step_ms))))
        fee_bps_rt = max(0.0, float(paper_cfg.get("fee_bps_round_trip", 4.0) or 4.0))
        slippage_bps_rt = max(0.0, float(paper_cfg.get("slippage_bps_round_trip", 2.0) or 2.0))
        max_spread_bps = max(0.0, float(paper_cfg.get("max_spread_bps", 0.0) or 0.0))
        # For labeling we only charge spread if an explicit spread-cost override is provided.
        # Otherwise max_spread_bps is treated as a gating cap, not an additive cost.
        spread_bps_used = max(0.0, float(paper_cfg.get("spread_cost_bps_used", 0.0) or 0.0))
        safety_buffer_bps = max(0.0, float(paper_cfg.get("safety_edge_buffer_bps", 2.0) or 2.0))
        ambiguity_margin_bps = max(0.0, float(paper_cfg.get("ambiguity_margin_bps", 0.0) or 0.0))
        min_edge_raw = (fee_bps_rt + slippage_bps_rt + spread_bps_used + safety_buffer_bps) / 10000.0
        # min_edge is net-profit buffer after costs; never allow it to exceed TP.
        min_edge = min(min_edge_raw, max(0.0, tb_tp))
        ambiguity_margin_pct = ambiguity_margin_bps / 10000.0
        barrier_debug = config.get("barrier_debug", {}) if isinstance(config.get("barrier_debug"), dict) else {}
        probe_enabled = bool(barrier_debug.get("enable_future_path_probe", True))
        probe_samples = max(1, min(20, int(barrier_debug.get("future_path_probe_samples", 5) or 5)))
        probe_depth = max(5, min(200, int(barrier_debug.get("future_path_probe_depth", 20) or 20)))
        step_tolerance_ms = max(1, min(60_000, int(barrier_debug.get("step_contiguity_tolerance_ms", 500) or 500)))
        feature_count = len(FEATURE_COLUMNS)
        potential_windows = max(0, (len(rows) - horizon) - lookback)
        memory_budget_bytes = self._training_memory_budget_bytes(config=config)
        min_viable_bytes = max(64 * 1024 * 1024, lookback * feature_count * 4 * 64)
        if memory_budget_bytes < min_viable_bytes:
            await self._emit_run_log(
                run_id,
                "ERROR",
                (
                    f"Configured RAM budget too low for minimum batch sizing. "
                    f"budget_mb={memory_budget_bytes / (1024 * 1024):.2f} "
                    f"required_mb={min_viable_bytes / (1024 * 1024):.2f}"
                ),
            )
            return None
        bytes_per_window = max(1, lookback * feature_count * 4 + 16)
        max_windows_by_memory = max(2000, int(memory_budget_bytes // bytes_per_window))
        strict_full_windows_mode = bool(config.get("strict_full_windows_mode", True))
        sampling_step = 1
        if (not strict_full_windows_mode) and potential_windows > max_windows_by_memory:
            sampling_step = int(math.ceil(potential_windows / max_windows_by_memory))
            await self._emit_run_log(
                run_id,
                "INFO",
                (
                    f"Memory guard active: potential_windows={potential_windows}, "
                    f"max_windows={max_windows_by_memory}, sampling_step={sampling_step}"
                ),
            )
        elif strict_full_windows_mode:
            await self._emit_run_log(
                run_id,
                "INFO",
                (
                    "Strict full windows mode enabled: "
                    f"potential_windows={potential_windows}, sampling_step=1"
                ),
            )

        selected_indices: list[int] = []
        dropped_windows_stale = 0
        window_trace["rows_rejected_lookback"] = int(max(0, lookback))
        window_trace["rows_rejected_horizon"] = int(max(0, horizon))
        for idx in range(lookback, len(rows) - horizon, sampling_step):
            window_trace["windows_attempted"] = int(window_trace["windows_attempted"]) + 1
            price_now = prices[idx]
            price_future = prices[idx + horizon]
            if price_now <= 0 or price_future <= 0:
                continue
            if any(bool(rows[k].get("is_stale_row", False)) for k in range(max(0, idx - lookback), idx + 1)):
                dropped_windows_stale += 1
                continue
            selected_indices.append(idx)
        window_trace["rows_rejected_stale_window"] = int(dropped_windows_stale)
        window_trace["windows_final"] = int(len(selected_indices))
        await self._emit_run_log(
            run_id,
            "INFO",
            f"Strict grid window filter: dropped_stale={dropped_windows_stale} kept={len(selected_indices)}",
        )
        await self._emit_run_log(
            run_id,
            "INFO",
            f"Window assembly from shards: attempted={int(window_trace['windows_attempted'])} final={int(window_trace['windows_final'])}",
        )
        if int(window_trace["registered_rows_total"]) > 0 and int(window_trace["windows_attempted"]) == 0:
            reason = "window_builder_using_empty_legacy_rows"
            if int(window_trace["usable_rows_after_timestamp_sort"]) <= 0:
                reason = "timestamp_sort_failed"
            elif len(rows) < (lookback + horizon + 1):
                reason = "not_enough_rows_for_lookback_horizon"
            elif int(window_trace["rows_rejected_missing_label_price"]) >= len(rows):
                reason = "label_price_series_missing"
            await self._emit_run_log(run_id, "ERROR", f"window_assembly_failed_zero_attempts reason={reason}")
            await self._merge_run_metrics(
                run_id,
                {
                    "window_assembly_trace": window_trace,
                    "zero_window_root_cause": reason,
                },
                emit_update=True,
            )
            return None
        await self._merge_run_metrics(
            run_id,
            {"window_assembly_trace": window_trace},
            emit_update=True,
        )
        if len(selected_indices) < 200:
            await self._emit_run_log(run_id, "WARNING", f"Too few training windows: {len(selected_indices)}")
            return None

        sample_count = len(selected_indices)
        ts_all = np.asarray([int(r.get("ts_ms", 0) or 0) for r in rows], dtype=np.int64)
        y_dir_arr = np.empty(sample_count, dtype=np.int64)
        y_mag_arr = np.empty(sample_count, dtype=np.float32)
        y_quality_arr = np.empty(sample_count, dtype=np.float32)
        ts_arr = np.empty(sample_count, dtype=np.int64)
        price_arr = np.empty(sample_count, dtype=np.float32)

        def _trade_outcome_direction_for_index(
            idx: int,
            min_edge_local: float,
            ambiguity_margin_local: float,
            spread_cap_bps_local: float,
        ) -> int:
            # Extra safety gates requested by ops:
            # 1) NO_TRADE when spread_bps exceeds cap
            # 2) NO_TRADE on stale windows (already filtered globally, keep explicit here)
            row_i = rows[idx]
            if bool(row_i.get("is_stale_row", False)):
                return 1
            cur_spread_bps = abs(float(row_i.get("spread", 0.0) or 0.0)) * 10000.0
            if spread_cap_bps_local > 0.0 and cur_spread_bps > spread_cap_bps_local:
                return 1
            price_now = prices[idx]
            tp_price = float(price_now * (1.0 + tb_tp))
            sl_price = float(price_now * (1.0 - tb_sl))
            entry_ts_ms = int(ts_all[idx]) if idx < len(ts_all) else 0
            if entry_ts_ms > 0:
                timeout_end_pos = int(np.searchsorted(ts_all, entry_ts_ms + int(timeout_ms), side="right") - 1)
                timeout_end_pos = max(idx + 1, min(timeout_end_pos, len(prices) - 1))
            else:
                timeout_end_pos = min(len(prices) - 1, idx + max(1, timeout_steps_effective))
            timeout_local = max(1, int(timeout_end_pos - idx))
            future_slice = prices[idx + 1 : idx + timeout_local + 1]
            long_ret = float((float(prices[idx + timeout_local]) - price_now) / max(1e-12, price_now)) if timeout_local > 0 else 0.0
            short_ret = -long_ret
            if future_slice.size > 0:
                for px in future_slice.tolist():
                    if px <= 0:
                        continue
                    if px >= tp_price:
                        long_ret = tb_tp
                        break
                    if px <= sl_price:
                        long_ret = -tb_sl
                        break
                for px in future_slice.tolist():
                    if px <= 0:
                        continue
                    if px <= sl_price:
                        short_ret = tb_tp
                        break
                    if px >= tp_price:
                        short_ret = -tb_sl
                        break
            long_net = float(long_ret - min_edge_local)
            short_net = float(short_ret - min_edge_local)
            # 3) NO_TRADE when both sides are too close (ambiguity band)
            if abs(long_net - short_net) <= ambiguity_margin_local:
                return 1
            if long_net >= 0.0 and long_net > (short_net + ambiguity_margin_local):
                return 2
            if short_net >= 0.0 and short_net > (long_net + ambiguity_margin_local):
                return 0
            return 1

        write_idx = 0
        tb_tp_hit_step_arr = np.full(sample_count, -1, dtype=np.int32)
        tb_sl_hit_step_arr = np.full(sample_count, -1, dtype=np.int32)
        tb_future_high_arr = np.zeros(sample_count, dtype=np.float32)
        tb_future_low_arr = np.zeros(sample_count, dtype=np.float32)
        tb_tie_count = 0
        tb_no_hit_count = 0
        tb_tp_first_count = 0
        tb_sl_first_count = 0
        for idx in selected_indices:
            price_now = prices[idx]
            price_future = prices[idx + horizon]
            ret = float((price_future - price_now) / price_now)
            entry_ts_ms = int(ts_all[idx]) if idx < len(ts_all) else 0
            if entry_ts_ms > 0:
                timeout_end_pos = int(np.searchsorted(ts_all, entry_ts_ms + int(timeout_ms), side="right") - 1)
                timeout_end_pos = max(idx + 1, min(timeout_end_pos, len(prices) - 1))
            else:
                timeout_end_pos = min(len(prices) - 1, idx + max(1, timeout_steps_effective))
            timeout = max(1, int(timeout_end_pos - idx))
            if target_mode == "trade_outcome":
                direction = _trade_outcome_direction_for_index(
                    idx=idx,
                    min_edge_local=min_edge,
                    ambiguity_margin_local=ambiguity_margin_pct,
                    spread_cap_bps_local=max_spread_bps,
                )
            elif target_mode == "triple_barrier":
                direction = 1
                tp_price = float(price_now * (1.0 + tb_tp))
                sl_price = float(price_now * (1.0 - tb_sl))
                tp_hit_step = -1
                sl_hit_step = -1
                future_slice = prices[idx + 1 : idx + timeout + 1]
                if future_slice.size > 0:
                    tb_future_high_arr[write_idx] = float(np.max(future_slice))
                    tb_future_low_arr[write_idx] = float(np.min(future_slice))
                else:
                    tb_future_high_arr[write_idx] = float(price_now)
                    tb_future_low_arr[write_idx] = float(price_now)
                for step in range(1, timeout + 1):
                    px = float(prices[idx + step])
                    if px <= 0:
                        continue
                    tp_hit = px >= tp_price
                    sl_hit = px <= sl_price
                    if tp_hit and tp_hit_step < 0:
                        tp_hit_step = step
                    if sl_hit and sl_hit_step < 0:
                        sl_hit_step = step
                    if tp_hit and sl_hit:
                        # Coarse-tick ambiguity tie: classify as FLAT for safety.
                        direction = 1
                        tb_tie_count += 1
                        break
                    if tp_hit:
                        direction = 2
                        tb_tp_first_count += 1
                        break
                    if sl_hit:
                        direction = 0
                        tb_sl_first_count += 1
                        break
                if direction == 1 and tp_hit_step < 0 and sl_hit_step < 0:
                    tb_no_hit_count += 1
                tb_tp_hit_step_arr[write_idx] = int(tp_hit_step)
                tb_sl_hit_step_arr[write_idx] = int(sl_hit_step)
            elif ret > threshold:
                direction = 2
            elif ret < -threshold:
                direction = 0
            else:
                direction = 1
            quality = 1.0 if abs(ret) > threshold * 2.0 else 0.0
            y_dir_arr[write_idx] = direction
            # Magnitude target is stored as fractional return (e.g. 0.0025 == 0.25%)
            # to keep regression scale stable and avoid inflated RMSE values.
            y_mag_arr[write_idx] = ret
            y_quality_arr[write_idx] = quality
            ts_arr[write_idx] = int(rows[idx].get("ts_ms", 0) or 0)
            price_arr[write_idx] = float(price_now)
            write_idx += 1

        if write_idx < 200:
            await self._emit_run_log(run_id, "WARNING", f"Too few valid training windows after build: {write_idx}")
            return None
        if write_idx != sample_count:
            y_dir_arr = y_dir_arr[:write_idx]
            y_mag_arr = y_mag_arr[:write_idx]
            y_quality_arr = y_quality_arr[:write_idx]
            ts_arr = ts_arr[:write_idx]
            price_arr = price_arr[:write_idx]
            selected_indices = selected_indices[:write_idx]

        n = write_idx
        label_counts = np.bincount(y_dir_arr[:n], minlength=3)
        # Always initialize diagnostics containers; they are merged into metrics
        # for all target modes (triple_barrier or trade_outcome).
        probe_rows: list[dict[str, Any]] = []
        spacing_probe_rows: list[dict[str, Any]] = []
        spacing_stats: dict[str, Any] = {}
        down_count = int(label_counts[0])
        flat_count = int(label_counts[1])
        up_count = int(label_counts[2])
        flat_rate = float(flat_count / max(1, n))
        if target_mode == "trade_outcome":
            directional_rate = float((down_count + up_count) / max(1, n))
            tuned_once = False
            if directional_rate > 0.35:
                safety_buffer_bps += 0.5
                ambiguity_margin_bps += 0.5
                tuned_once = True
                await self._emit_run_log(
                    run_id,
                    "WARNING",
                    (
                        "Trade-outcome auto-tighten triggered before training: "
                        f"directional_rate={directional_rate*100.0:.2f}% -> safety_edge_buffer_bps={safety_buffer_bps:.4f}, "
                        f"ambiguity_margin_bps={ambiguity_margin_bps:.4f}"
                    ),
                )
            elif directional_rate < 0.03:
                safety_buffer_bps = max(0.0, safety_buffer_bps - 0.5)
                if max_hold_ms_cfg < 300_000:
                    max_hold_ms_cfg = 300_000
                    timeout_ms = max(timeout_ms, 300_000)
                    timeout_steps_effective = max(1, int(round(timeout_ms / max(1, median_step_ms))))
                tuned_once = True
                await self._emit_run_log(
                    run_id,
                    "WARNING",
                    (
                        "Trade-outcome auto-loosen triggered before training: "
                        f"directional_rate={directional_rate*100.0:.2f}% -> safety_edge_buffer_bps={safety_buffer_bps:.4f}, "
                        f"max_hold_seconds={int(timeout_ms/1000)}"
                    ),
                )
            if tuned_once:
                min_edge_raw = (fee_bps_rt + slippage_bps_rt + spread_bps_used + safety_buffer_bps) / 10000.0
                min_edge = min(min_edge_raw, max(0.0, tb_tp))
                ambiguity_margin_pct = ambiguity_margin_bps / 10000.0
                for i_pos, idx in enumerate(selected_indices):
                    y_dir_arr[i_pos] = _trade_outcome_direction_for_index(
                        idx=idx,
                        min_edge_local=min_edge,
                        ambiguity_margin_local=ambiguity_margin_pct,
                        spread_cap_bps_local=max_spread_bps,
                    )
                label_counts = np.bincount(y_dir_arr[:n], minlength=3)
                down_count = int(label_counts[0])
                flat_count = int(label_counts[1])
                up_count = int(label_counts[2])
                flat_rate = float(flat_count / max(1, n))
            await self._emit_run_log(
                run_id,
                "INFO",
                (
                    "Target mode: trade_outcome "
                    f"(tp={tb_tp*100.0:.4f}% sl={tb_sl*100.0:.4f}% timeout_ms={timeout_ms} min_edge={min_edge*100.0:.4f}%)"
                ),
            )
            await self._emit_run_log(
                run_id,
                "INFO",
                (
                    "Trade-outcome label components: "
                    f"tp_pct={tb_tp*100.0:.4f}% "
                    f"sl_pct={tb_sl*100.0:.4f}% "
                    f"fee_bps={fee_bps_rt:.4f} "
                    f"spread_bps_used={spread_bps_used:.4f} "
                    f"slippage_bps={slippage_bps_rt:.4f} "
                    f"safety_buffer_bps={safety_buffer_bps:.4f} "
                    f"max_spread_bps={max_spread_bps:.4f} "
                    f"min_edge_pct={min_edge*100.0:.4f}% "
                    f"ambiguity_margin_pct={ambiguity_margin_pct*100.0:.4f}%"
                ),
            )
            await self._emit_run_log(
                run_id,
                "INFO",
                (
                    "Trade-outcome labels: "
                    f"short_good={down_count}, no_trade={flat_count}, long_good={up_count}, no_trade_rate={flat_rate*100.0:.2f}% "
                    f"min_edge={min_edge*100.0:.4f}%"
                ),
            )
        else:
            await self._emit_run_log(
                run_id,
                "INFO",
                (
                    "Triple barrier labels: "
                    f"down={down_count}, flat={flat_count}, up={up_count}, flat_rate={flat_rate*100.0:.2f}%"
                ),
            )
        if target_mode == "triple_barrier":
            await self._emit_run_log(
                run_id,
                "INFO",
                (
                    "Triple barrier hit stats: "
                    f"tp={tb_tp*100.0:.4f}% sl={tb_sl*100.0:.4f}% timeout_steps={timeout_steps_effective} timeout_ms={timeout_ms} windows_total={n} "
                    f"tp_first_count={tb_tp_first_count} sl_first_count={tb_sl_first_count} "
                    f"no_hit_count={tb_no_hit_count} tie_count={tb_tie_count}"
                ),
            )
            await self._emit_run_log(run_id, "INFO", f"Triple barrier timeout interpreted as time-based: timeout_ms={timeout_ms}, approx_steps={timeout_steps_effective}.")
            probe_n = min(20, n)
            if probe_n > 0:
                # Deterministic spread over timeline
                probe_positions = np.linspace(0, n - 1, probe_n, dtype=np.int64)
                for p in probe_positions.tolist():
                    idx_src = int(selected_indices[p])
                    entry_price = float(prices[idx_src])
                    entry_ts = int(rows[idx_src].get("ts_ms", 0) or 0)
                    if entry_ts > 0:
                        end_pos = int(np.searchsorted(ts_all, entry_ts + int(timeout_ms), side="right") - 1)
                        end_pos = max(idx_src + 1, min(end_pos, len(prices) - 1))
                    else:
                        end_pos = min(len(prices) - 1, idx_src + max(1, timeout_steps_effective))
                    timeout = max(1, int(end_pos - idx_src))
                    tp_price = float(entry_price * (1.0 + tb_tp))
                    sl_price = float(entry_price * (1.0 - tb_sl))
                    dir_i = int(y_dir_arr[p])
                    assigned_label = "flat"
                    if dir_i == 2:
                        assigned_label = "up"
                    elif dir_i == 0:
                        assigned_label = "down"
                    probe_rows.append(
                        {
                            "sample_pos": int(p),
                            "entry_price": float(round(entry_price, 8)),
                            "tp_price": float(round(tp_price, 8)),
                            "sl_price": float(round(sl_price, 8)),
                            "future_high_max": float(round(float(tb_future_high_arr[p]), 8)),
                            "future_low_min": float(round(float(tb_future_low_arr[p]), 8)),
                            "tp_hit_step": int(tb_tp_hit_step_arr[p]),
                            "sl_hit_step": int(tb_sl_hit_step_arr[p]),
                            "timeout_steps_used": int(timeout),
                            "assigned_label": assigned_label,
                        }
                    )
            if probe_enabled and n > 0:
                path_positions = np.linspace(0, n - 1, min(probe_samples, n), dtype=np.int64)
                all_step_deltas: list[int] = []
                non_monotonic = 0
                for p in path_positions.tolist():
                    idx_src = int(selected_indices[p])
                    entry_ts = int(rows[idx_src].get("ts_ms", 0) or 0)
                    if entry_ts > 0:
                        end_pos = int(np.searchsorted(ts_all, entry_ts + int(timeout_ms), side="right") - 1)
                        end_pos = max(idx_src + 1, min(end_pos, len(prices) - 1))
                    else:
                        end_pos = min(len(prices) - 1, idx_src + max(1, timeout_steps_effective))
                    timeout = max(1, int(end_pos - idx_src))
                    end_idx = min(idx_src + timeout, len(rows) - 1)
                    ts_vals: list[int] = []
                    px_vals: list[float] = []
                    for j in range(idx_src + 1, end_idx + 1):
                        ts_vals.append(int(rows[j].get("ts_ms", 0) or 0))
                        px_vals.append(float(prices[j]))
                    ts_trim = ts_vals[:probe_depth]
                    px_trim = px_vals[:probe_depth]
                    delta_trim = [int(t - entry_ts) for t in ts_trim]
                    step_delta_trim: list[int] = []
                    prev_ts = None
                    for t in ts_trim:
                        if prev_ts is not None:
                            d = int(t - prev_ts)
                            step_delta_trim.append(d)
                            all_step_deltas.append(d)
                            if d <= 0:
                                non_monotonic += 1
                        prev_ts = t
                    spacing_probe_rows.append(
                        {
                            "sample_pos": int(p),
                            "entry_price": float(round(float(prices[idx_src]), 8)),
                            "future_prices_0_20": [float(round(v, 8)) for v in px_trim],
                            "future_timestamps_0_20": [int(v) for v in ts_trim],
                            "time_delta_ms_0_20": [int(v) for v in delta_trim],
                            "step_delta_ms_0_20": [int(v) for v in step_delta_trim],
                            "timeout_steps_used": int(timeout),
                        }
                    )
                if all_step_deltas:
                    arr = np.asarray(all_step_deltas, dtype=np.int64)
                    med = float(np.median(arr))
                    min_d = int(np.min(arr))
                    max_d = int(np.max(arr))
                    contiguous = bool(non_monotonic == 0 and max_d <= step_tolerance_ms)
                    spacing_stats = {
                        "median_step_delta_ms": med,
                        "min_step_delta_ms": min_d,
                        "max_step_delta_ms": max_d,
                        "non_monotonic_timestamp_count": int(non_monotonic),
                        "contiguous_within_tolerance": contiguous,
                        "step_contiguity_tolerance_ms": int(step_tolerance_ms),
                        "estimated_timeout_ms": float(timeout_steps_effective * med),
                        "estimated_timeout_seconds": float((timeout_steps_effective * med) / 1000.0),
                    }
                    if (not contiguous) or non_monotonic > 0:
                        await self._emit_run_log(
                            run_id,
                            "WARNING",
                            (
                                "Triple barrier future-step spacing anomaly: "
                                f"median={med:.1f}ms min={min_d}ms max={max_d}ms "
                                f"non_monotonic={non_monotonic} tolerance={step_tolerance_ms}ms"
                            ),
                        )
                    await self._emit_run_log(
                        run_id,
                        "INFO",
                        (
                            f"Triple barrier spacing: median_step_delta_ms={med:.1f} "
                            f"estimated_timeout_seconds={(timeout_steps_effective * med) / 1000.0:.3f}"
                        ),
                    )
            await self._emit_run_log(
                run_id,
                "INFO",
                "Triple barrier debug probe (20 windows): " + json.dumps(probe_rows, separators=(",", ":")),
            )
            if probe_enabled:
                await self._emit_run_log(
                    run_id,
                    "INFO",
                    "Triple barrier future-path probe (5 samples): " + json.dumps(spacing_probe_rows, separators=(",", ":")),
                )
        label_guard_enabled = bool(config.get("label_guard_enabled", True))
        min_flat_rate_required = float((config.get("paper_bot", {}) or {}).get("min_flat_rate_required", 0.12) or 0.12)
        flat_guard_triggered = bool(
            target_mode == "triple_barrier"
            and label_guard_enabled
            and (flat_count == 0 or flat_rate < min_flat_rate_required)
        )
        await self._merge_run_metrics(
            run_id,
            {
                "label_price_source_used": "bookticker_mid_state",
                "label_price_anomaly_count": int(anomaly_count),
                "label_price_anomaly_rate": float(anomaly_count / max(1, len(prices))),
                "label_price_anomaly_samples": anomaly_samples,
                "label_price_forward_fill_rate": float(label_price_ffill_rate),
                "bookticker_updates_total": int(bookticker_updates_total),
                "label_price_rejected_count": int(label_price_rejected_count),
                "label_price_rejected_rate": float(label_price_rejected_rate),
                "max_label_price_rejected_rate_used": float(max_label_price_rejected_rate),
                "label_price_quality_guard_triggered": bool(label_price_quality_guard_triggered),
                "label_counts": {"down": down_count, "flat": flat_count, "up": up_count},
                "trade_outcome_label_counts": (
                    {"short_good": down_count, "no_trade": flat_count, "long_good": up_count}
                    if target_mode == "trade_outcome"
                    else {}
                ),
                "flat_rate": float(round(flat_rate, 6)),
                "trade_outcome_label_components": (
                    {
                        "tp_pct": float(tb_tp * 100.0),
                        "sl_pct": float(tb_sl * 100.0),
                        "fee_bps": float(fee_bps_rt),
                        "spread_bps_used": float(spread_bps_used),
                        "slippage_bps": float(slippage_bps_rt),
                        "safety_buffer_bps": float(safety_buffer_bps),
                        "max_spread_bps": float(max_spread_bps),
                        "min_edge_pct": float(min_edge * 100.0),
                        "ambiguity_margin_pct": float(ambiguity_margin_pct * 100.0),
                    }
                    if target_mode == "trade_outcome"
                    else {}
                ),
                "label_guard_enabled_used": bool(label_guard_enabled),
                "flat_guard_triggered": bool(flat_guard_triggered),
                "flat_guard_min_required": float(min_flat_rate_required),
                "triple_barrier_hit_stats": {
                    "tp_first_count": int(tb_tp_first_count),
                    "sl_first_count": int(tb_sl_first_count),
                    "no_hit_count": int(tb_no_hit_count),
                    "tie_count": int(tb_tie_count),
                    "flat_rate": float(round(flat_rate, 6)),
                    "tp_pct_used": float(tb_tp * 100.0),
                    "sl_pct_used": float(tb_sl * 100.0),
                    "timeout_steps_used": int(tb_timeout),
                    "windows_total": int(n),
                },
                "triple_barrier_probe_rows": probe_rows if target_mode == "triple_barrier" else [],
                "triple_barrier_future_path_probe_rows": spacing_probe_rows if target_mode == "triple_barrier" else [],
                "triple_barrier_step_spacing_stats": spacing_stats if target_mode == "triple_barrier" else {},
                "estimated_timeout_seconds": float((spacing_stats.get("estimated_timeout_seconds", 0.0) if isinstance(spacing_stats, dict) else 0.0)),
            },
            emit_update=True,
        )
        if flat_guard_triggered:
            await self._mark_failed(
                run_id,
                (
                    "Triple-barrier label guard failed before training: "
                    f"down={down_count}, flat={flat_count}, up={up_count}, flat_rate={flat_rate*100.0:.2f}% "
                    f"(min_required={min_flat_rate_required*100.0:.2f}%, "
                    f"tp={tb_tp*100.0:.4f}% sl={tb_sl*100.0:.4f}% timeout={tb_timeout})"
                ),
            )
            return None

        train_end = int(n * 0.70)
        val_end = int(n * 0.85)
        dropped_window_rate = float(dropped_windows_stale / max(1, dropped_windows_stale + len(selected_indices)))

        def _probe_trade_outcome_hit_order(idx: int) -> dict[str, Any]:
            """Return side-specific hit-order diagnostics for trade_outcome label auditing."""
            price_now = float(prices[idx])
            entry_ts_ms = int(ts_all[idx]) if idx < len(ts_all) else 0
            if entry_ts_ms > 0:
                timeout_end_pos = int(np.searchsorted(ts_all, entry_ts_ms + int(timeout_ms), side="right") - 1)
                timeout_end_pos = max(idx + 1, min(timeout_end_pos, len(prices) - 1))
            else:
                timeout_end_pos = min(len(prices) - 1, idx + max(1, timeout_steps_effective))
            timeout_local = max(1, int(timeout_end_pos - idx))
            future_slice = prices[idx + 1 : idx + timeout_local + 1]

            def _scan(*, is_long: bool) -> dict[str, Any]:
                if is_long:
                    tp_price = float(price_now * (1.0 + tb_tp))
                    sl_price = float(price_now * (1.0 - tb_sl))
                else:
                    tp_price = float(price_now * (1.0 - tb_sl))
                    sl_price = float(price_now * (1.0 + tb_tp))
                tp_step = -1
                sl_step = -1
                both_touched = False
                for step, px in enumerate(future_slice.tolist(), start=1):
                    if px <= 0:
                        continue
                    if is_long:
                        tp_hit = px >= tp_price
                        sl_hit = px <= sl_price
                    else:
                        tp_hit = px <= tp_price
                        sl_hit = px >= sl_price
                    if tp_hit and tp_step < 0:
                        tp_step = step
                    if sl_hit and sl_step < 0:
                        sl_step = step
                    if tp_step >= 0 and sl_step >= 0:
                        both_touched = True
                        break
                tp_before_sl = bool(tp_step >= 0 and (sl_step < 0 or tp_step < sl_step))
                sl_before_tp = bool(sl_step >= 0 and (tp_step < 0 or sl_step < tp_step))
                timeout_used = bool(tp_step < 0 and sl_step < 0)
                ambiguity = bool(both_touched and abs(tp_step - sl_step) <= 1)
                round_trip_cost_pct = float((fee_bps_rt + slippage_bps_rt + spread_bps_used) / 10000.0)
                if is_long:
                    raw_ret = float((prices[idx + timeout_local] - price_now) / max(1e-12, price_now))
                else:
                    raw_ret = float((price_now - prices[idx + timeout_local]) / max(1e-12, price_now))
                return {
                    "tp_before_sl": tp_before_sl,
                    "sl_before_tp": sl_before_tp,
                    "timeout_used": timeout_used,
                    "both_tp_sl_touched": bool(both_touched),
                    "ambiguity": ambiguity,
                    "tp_step": int(tp_step),
                    "sl_step": int(sl_step),
                    "realized_return_after_cost_pct": float(raw_ret - round_trip_cost_pct),
                    "timeout_local": int(timeout_local),
                }

            return {"long_good": _scan(is_long=True), "short_good": _scan(is_long=False)}

        label_probe_summary: dict[str, Any] = {}
        if target_mode == "trade_outcome":
            split_ranges = {"train": (0, train_end), "val": (train_end, val_end), "test": (val_end, n)}
            label_probe_summary = {"train": {}, "val": {}, "test": {}}
            for split_name, (start_idx, end_idx) in split_ranges.items():
                split_summary: dict[str, Any] = {}
                for lbl_name, lbl_val in (("long_good", 2), ("short_good", 0), ("no_trade", 1)):
                    count = 0
                    tp_before_sl = 0
                    sl_before_tp = 0
                    timeout_used = 0
                    both_tp_sl_touched = 0
                    ambiguity = 0
                    realized_returns: list[float] = []
                    for pos in range(start_idx, end_idx):
                        if int(y_dir_arr[pos]) != lbl_val:
                            continue
                        count += 1
                        if lbl_name == "no_trade":
                            realized_returns.append(0.0)
                            continue
                        probe = _probe_trade_outcome_hit_order(int(selected_indices[pos]))[lbl_name]
                        tp_before_sl += int(bool(probe["tp_before_sl"]))
                        sl_before_tp += int(bool(probe["sl_before_tp"]))
                        timeout_used += int(bool(probe["timeout_used"]))
                        both_tp_sl_touched += int(bool(probe["both_tp_sl_touched"]))
                        ambiguity += int(bool(probe["ambiguity"]))
                        realized_returns.append(float(probe["realized_return_after_cost_pct"]))
                    n_lbl = max(1, count)
                    rr_arr = np.asarray(realized_returns, dtype=np.float64) if realized_returns else np.asarray([], dtype=np.float64)
                    split_summary[lbl_name] = {
                        "count": int(count),
                        "tp_before_sl_pct": float(tp_before_sl / n_lbl),
                        "sl_before_tp_pct": float(sl_before_tp / n_lbl),
                        "timeout_pct": float(timeout_used / n_lbl),
                        "both_tp_sl_touched_rate": float(both_tp_sl_touched / n_lbl),
                        "ambiguity_rate": float(ambiguity / n_lbl),
                        "avg_realized_return_after_cost_pct": float(np.mean(rr_arr)) if rr_arr.size else 0.0,
                        "median_realized_return_after_cost_pct": float(np.median(rr_arr)) if rr_arr.size else 0.0,
                        "p10_realized_return_after_cost_pct": float(np.percentile(rr_arr, 10)) if rr_arr.size else 0.0,
                        "p90_realized_return_after_cost_pct": float(np.percentile(rr_arr, 90)) if rr_arr.size else 0.0,
                    }
                label_probe_summary[split_name] = split_summary
        run_cache_dir = self._dataset_cache_root / str(run_id)
        run_cache_dir.mkdir(parents=True, exist_ok=True)
        normalized_features_path = run_cache_dir / "normalized_features.npy"
        np.save(normalized_features_path, normalized, allow_pickle=False)
        # Re-open in mmap mode so downstream training/eval can page from disk instead of
        # keeping a second large in-memory matrix alive.
        normalized = np.load(normalized_features_path, mmap_mode="r")
        dataset = {
            "normalized_features": [],
            "normalized_features_path": str(normalized_features_path),
            "window_indices_train": np.asarray(selected_indices[:train_end], dtype=np.int64),
            "window_indices_val": np.asarray(selected_indices[train_end:val_end], dtype=np.int64),
            "window_indices_test": np.asarray(selected_indices[val_end:], dtype=np.int64),
            "y_dir_train": y_dir_arr[:train_end],
            "y_dir_val": y_dir_arr[train_end:val_end],
            "y_dir_test": y_dir_arr[val_end:],
            "y_mag_train": y_mag_arr[:train_end],
            "y_mag_val": y_mag_arr[train_end:val_end],
            "y_mag_test": y_mag_arr[val_end:],
            "y_quality_train": y_quality_arr[:train_end],
            "y_quality_val": y_quality_arr[train_end:val_end],
            "y_quality_test": y_quality_arr[val_end:],
            "ts_train": ts_arr[:train_end],
            "ts_val": ts_arr[train_end:val_end],
            "ts_test": ts_arr[val_end:],
            "price_train": price_arr[:train_end],
            "price_val": price_arr[train_end:val_end],
            "price_test": price_arr[val_end:],
            "feature_names": FEATURE_COLUMNS,
            "window_assembly_trace": dict(window_trace),
            "label_probe_summary": label_probe_summary,
            "lookback_steps": lookback,
            "horizon": horizon,
            "threshold": threshold,
            "target_mode": target_mode,
            "triple_barrier": {"tp_pct": tb_tp * 100.0, "sl_pct": tb_sl * 100.0, "timeout_steps": tb_timeout},
            "samples_total": n,
            "sampling_step": sampling_step,
            "memory_budget_mb": round(memory_budget_bytes / (1024 * 1024), 2),
            "strict_full_windows_mode": strict_full_windows_mode,
            "strict_grid_interval_ms": int(max(1, median_step_ms)),
            "strict_grid_target_sample_ms": int(max(100, int(config.get("_effective_sample_ms", 100) or 100))),
            "strict_grid_stale_row_count": int(stale_rows_total),
            "strict_grid_stale_row_rate": float(stale_rows_rate),
            "strict_grid_dropped_windows_stale": int(dropped_windows_stale),
            "strict_grid_dropped_window_rate": float(dropped_window_rate),
        }
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                f"Dataset ready. windows={n} train={train_end} val={val_end-train_end} test={n-val_end} "
                f"sampling_step={sampling_step} strict_full={strict_full_windows_mode} budget={dataset['memory_budget_mb']}MB"
            ),
        )
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                f"Strict grid diagnostics: target_interval_ms={dataset['strict_grid_target_sample_ms']} "
                f"actual_median_step_ms={dataset['strict_grid_interval_ms']} "
                f"stale_row_rate={dataset['strict_grid_stale_row_rate']*100.0:.2f}% "
                f"dropped_window_rate={dataset['strict_grid_dropped_window_rate']*100.0:.2f}%"
            ),
        )
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                f"Effective usable rows/windows: train={train_end} val={val_end-train_end} test={n-val_end}"
            ),
        )
        await self._emit_run_log(
            run_id,
            "INFO",
            "Magnitude target scale: fractional return (1.0 == 100%).",
        )
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                f"Global split boundaries: windows_total={n}, train_end_idx={train_end}, "
                f"val_end_idx={val_end}, lookback={lookback}, horizon={horizon}"
            ),
        )
        if label_probe_summary:
            await self._emit_run_log(run_id, "INFO", "Label probe summary exported for trade_outcome splits train/val/test.")
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                f"Target mode: {target_mode}"
                + (
                    f" (tp={tb_tp*100.0:.4f}% sl={tb_sl*100.0:.4f}% timeout_ms={timeout_ms})"
                    if target_mode == "triple_barrier"
                    else (
                        f" (min_edge={min_edge*100.0:.4f}% max_hold_ms={timeout_ms})"
                        if target_mode == "trade_outcome"
                        else f" (fixed_return_threshold={threshold*100.0:.4f}%)"
                    )
                )
            ),
        )
        loaded_now_gb = float((feature_matrix.nbytes + normalized.nbytes) / float(1024**3))
        total_data_gb = float((config.get("_planner_total_data_bytes", 0) or 0) / float(1024**3))
        if total_data_gb <= 0:
            total_data_gb = float(loaded_now_gb)
        await self._merge_run_metrics(
            run_id,
            {
                "loaded_now_gb": float(round(loaded_now_gb, 6)),
                "windows_total": int(n),
                "windows_processed": 0,
                "strict_full_windows_mode": bool(strict_full_windows_mode),
                "used_data_gb": float(round(total_data_gb, 6)),
                "target_mode_used": str(target_mode),
                "tp_pct_used": float(tb_tp * 100.0),
                "sl_pct_used": float(tb_sl * 100.0),
                "timeout_steps_used": int(tb_timeout),
                "timeout_ms_used": int(timeout_ms),
                "strict_grid_target_interval_ms": int(dataset["strict_grid_target_sample_ms"]),
                "strict_grid_median_step_ms": int(dataset["strict_grid_interval_ms"]),
                "strict_grid_stale_row_count": int(dataset["strict_grid_stale_row_count"]),
                "strict_grid_stale_row_rate": float(dataset["strict_grid_stale_row_rate"]),
                "strict_grid_dropped_windows_stale": int(dataset["strict_grid_dropped_windows_stale"]),
                "strict_grid_dropped_window_rate": float(dataset["strict_grid_dropped_window_rate"]),
            },
            emit_update=True,
        )
        return dataset

    def _list_market_event_chunks(
        self,
        *,
        pair_symbol: str,
        ts_start_ms: int | None = None,
        ts_end_ms: int | None = None,
        limit: int = 5000,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        if not self._terminal_sqlite_path.exists():
            return []
        where = ["pair_symbol = ?"]
        params: list[Any] = [pair_symbol.upper()]
        if ts_start_ms is not None and ts_start_ms > 0:
            where.append("ts_end_ms >= ?")
            params.append(int(ts_start_ms))
        if ts_end_ms is not None and ts_end_ms > 0:
            where.append("ts_start_ms <= ?")
            params.append(int(ts_end_ms))
        sql = (
            "SELECT pair_symbol, source_exchange_id, ts_start_ms, ts_end_ms, file_path, row_count, size_bytes "
            "FROM market_event_chunks "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY CASE WHEN file_path LIKE '%_compacted%' THEN 0 ELSE 1 END, ts_start_ms ASC "
            "LIMIT ? OFFSET ?"
        )
        params.append(max(1, int(limit)))
        params.append(max(0, int(offset)))
        try:
            with sqlite3.connect(self._terminal_sqlite_path) as conn:
                rows = conn.execute(sql, tuple(params)).fetchall()
        except Exception:
            return []
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "pair_symbol": str(row[0] or "").upper(),
                    "source_exchange_id": str(row[1] or "unknown").lower(),
                    "ts_start_ms": int(row[2] or 0),
                    "ts_end_ms": int(row[3] or 0),
                    "file_path": str(row[4] or ""),
                    "row_count": int(row[5] or 0),
                    "size_bytes": int(row[6] or 0),
                }
            )
        return out

    async def _market_events_data_status(self, pair_symbol: str) -> dict[str, Any]:
        chunks = self._list_market_event_chunks(pair_symbol=pair_symbol, limit=200000)
        if not chunks:
            return {
                "pair_symbol": pair_symbol,
                "file_count": 0,
                "rows_total": 0,
                "min_ts_ms": None,
                "max_ts_ms": None,
                "data_hours": 0.0,
                "total_data_hours_available": 0.0,
                "window_anchor_min_ts_ms": None,
                "window_anchor_max_ts_ms": None,
                "hour_slot_count": 0,
                "active_source": "local_market_events",
                "active_source_path": str(self._market_events_root / f"pair={pair_symbol.upper()}"),
            }
        file_count = len(chunks)
        rows_total = int(sum(int(item.get("row_count", 0) or 0) for item in chunks))
        min_ts_ms = min(int(item.get("ts_start_ms", 0) or 0) for item in chunks)
        max_ts_ms = max(int(item.get("ts_end_ms", 0) or 0) for item in chunks)
        data_hours = 0.0
        if min_ts_ms > 0 and max_ts_ms > min_ts_ms:
            data_hours = (max_ts_ms - min_ts_ms) / 3_600_000
        hour_slot_count = 0
        if min_ts_ms > 0 and max_ts_ms >= min_ts_ms:
            hour_slot_count = max(1, int(((max_ts_ms - min_ts_ms) // 3_600_000) + 1))
        return {
            "pair_symbol": pair_symbol,
            "file_count": file_count,
            "rows_total": rows_total,
            "min_ts_ms": min_ts_ms,
            "max_ts_ms": max_ts_ms,
            "data_hours": data_hours,
            "total_data_hours_available": data_hours,
            "window_anchor_min_ts_ms": min_ts_ms,
            "window_anchor_max_ts_ms": max_ts_ms,
            "hour_slot_count": hour_slot_count,
            "active_source": "local_market_events",
            "active_source_path": str(self._market_events_root / f"pair={pair_symbol.upper()}"),
        }

    def _apply_training_hour_window(
        self,
        events: list[dict[str, Any]],
        config: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        enabled = bool(config.get("training_hour_window_enabled", False))
        requested_start = max(0, int(config.get("training_hour_start", 0) or 0))
        requested_end = max(0, int(config.get("training_hour_end", 0) or 0))
        meta = {
            "enabled": enabled,
            "requested_start_hour": requested_start,
            "requested_end_hour": requested_end,
            "effective_start_hour": requested_start,
            "effective_end_hour": requested_end,
            "effective_start_ts_ms": None,
            "effective_end_ts_ms": None,
        }
        if not enabled or not events:
            return events, meta
        ts_values = [int(item.get("ts_receive_ms", 0) or 0) for item in events]
        ts_values = [val for val in ts_values if val > 0]
        if not ts_values:
            return events, meta
        anchor_min = min(ts_values)
        anchor_max = max(ts_values)
        if requested_end != 0 and requested_end < requested_start:
            requested_end = requested_start
        start_ts = anchor_min + (requested_start * 3_600_000)
        if requested_end == 0:
            end_ts = anchor_max
            effective_end_hour = max(requested_start, int((anchor_max - anchor_min) // 3_600_000))
        else:
            end_ts = anchor_min + ((requested_end + 1) * 3_600_000) - 1
            effective_end_hour = requested_end
        meta["effective_end_hour"] = effective_end_hour
        meta["effective_start_ts_ms"] = int(start_ts)
        meta["effective_end_ts_ms"] = int(end_ts)
        filtered = [
            item
            for item in events
            if start_ts <= int(item.get("ts_receive_ms", 0) or 0) <= end_ts
        ]
        return filtered, meta

    @staticmethod
    def _parse_depth_levels(raw_levels: Any, max_levels: int = 10) -> list[tuple[float, float]]:
        levels: list[tuple[float, float]] = []
        if not isinstance(raw_levels, list):
            return levels
        for item in raw_levels:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                continue
            try:
                price = float(item[0] or 0.0)
                qty = float(item[1] or 0.0)
            except Exception:
                continue
            if price <= 0 or qty <= 0:
                continue
            levels.append((price, qty))
            if len(levels) >= max_levels:
                break
        return levels

    def _market_events_to_feature_rows(
        self,
        events: list[dict[str, Any]],
        sample_ms_override: int | None = None,
    ) -> list[dict[str, Any]]:
        sample_ms = max(100, int(sample_ms_override if sample_ms_override is not None else os.getenv("ML_MARKET_EVENT_SAMPLE_MS", "200")))
        strict_grid_enabled = True
        max_ffill_gap_ms = max(sample_ms * 2, int(os.getenv("ML_GRID_MAX_FFILL_GAP_MS", "1500")))
        stale_book_ms = max_ffill_gap_ms
        stale_trade_ms = max(sample_ms * 5, int(os.getenv("ML_GRID_STALE_TRADE_MS", "3000")))
        stale_depth_ms = max(sample_ms * 5, int(os.getenv("ML_GRID_STALE_DEPTH_MS", "3000")))
        trade_window_ms = 10_000
        liq_window_ms = 60_000

        trade_ts: deque[int] = deque()
        liq_ts: deque[int] = deque()
        liq_notional_60_window: deque[tuple[int, float]] = deque()
        liq_notional_10_window: deque[tuple[int, float]] = deque()
        liq_buy_notional_60_window: deque[tuple[int, float]] = deque()
        liq_sell_notional_60_window: deque[tuple[int, float]] = deque()
        buy_qty_window: deque[tuple[int, float]] = deque()
        sell_qty_window: deque[tuple[int, float]] = deque()
        buy_qty_sum = 0.0
        sell_qty_sum = 0.0
        liq_notional_60_sum = 0.0
        liq_notional_10_sum = 0.0
        liq_buy_notional_60_sum = 0.0
        liq_sell_notional_60_sum = 0.0
        notional_window: deque[tuple[int, float]] = deque()
        qty_window: deque[tuple[int, float]] = deque()
        qty_window_30: deque[tuple[int, float]] = deque()
        notional_sum = 0.0
        qty_sum = 0.0
        qty_sum_30 = 0.0
        mid_history: deque[tuple[int, float]] = deque()
        oi_history: deque[tuple[int, float]] = deque()
        spread_history: deque[tuple[int, float]] = deque()
        funding_history: deque[tuple[int, float]] = deque()

        best_bid = 0.0
        best_ask = 0.0
        bid_depth_top10 = 0.0
        ask_depth_top10 = 0.0
        prev_depth_bids: dict[float, float] = {}
        prev_depth_asks: dict[float, float] = {}
        cancel_proxy_window: deque[tuple[int, float]] = deque()
        passive_add_window: deque[tuple[int, float]] = deque()
        cancel_proxy_sum = 0.0
        passive_add_sum = 0.0
        funding_rate = 0.0
        open_interest = 0.0
        last_mid = 0.0
        last_label_price = 0.0
        last_label_price_ts_ms = 0
        label_price_ffill_count = 0
        label_price_direct_count = 0
        label_price_rejected_count = 0
        bookticker_updates_total = 0
        label_price_rejection_samples: list[dict[str, Any]] = []
        label_price_reset_accept_count = 0
        last_spread = 0.0
        nearest_bid_wall_distance_pct = 0.0
        nearest_ask_wall_distance_pct = 0.0
        last_book_ts_ms = 0
        last_trade_ts_ms = 0
        last_depth_ts_ms = 0

        grid_next_ts = 0
        rows: list[dict[str, Any]] = []

        stats = {
            "events_total": 0,
            "events_tool_filtered": 0,
            "events_json_error": 0,
            "events_bookticker": 0,
            "events_depth": 0,
            "events_trade": 0,
            "events_mark": 0,
            "events_liq": 0,
            "skip_no_mid": 0,
            "skip_sample_gate": 0,
            "strict_grid_enabled": 1,
            "grid_interval_ms": int(sample_ms),
            "max_ffill_gap_ms": int(max_ffill_gap_ms),
            "stale_row_count": 0,
            "rows_emitted": 0,
            "grid_gap_resync_count": 0,
            "grid_gap_rows_skipped_estimate": 0,
            "scanned_events": 0,
            "after_exchange_filter": 0,
            "after_event_type_filter": 0,
            "after_symbol_filter": 0,
            "after_timestamp_validity_filter": 0,
            "events_unknown": 0,
            "grid_rows_attempted": 0,
            "rows_with_valid_book": 0,
            "rows_with_valid_spread": 0,
            "rows_with_recent_trade": 0,
            "rows_rejected_stale_book": 0,
            "rows_rejected_stale_trade": 0,
            "rows_rejected_spread": 0,
            "rows_rejected_missing_price": 0,
            "rows_rejected_missing_features": 0,
        }
        max_grid_gap_ms = int(os.getenv("ML_MAX_GRID_GAP_MS", "300000"))
        if max_grid_gap_ms < sample_ms:
            max_grid_gap_ms = sample_ms

        for event in events:
            stats["scanned_events"] += 1
            stats["events_total"] += 1
            ts_ms = int(event.get("ts_receive_ms", 0) or event.get("ts_exchange_ms", 0) or 0)
            if ts_ms <= 0:
                continue
            stats["after_timestamp_validity_filter"] += 1
            stats["after_exchange_filter"] += 1
            stats["after_symbol_filter"] += 1
            stream = str(event.get("stream", "") or "").lower()
            event_type = str(event.get("event_type", "") or "").lower()
            tool_id = event_stream_to_tool_id(stream, event_type)
            if tool_id and (not self._mode_tool_enabled("training", tool_id)):
                stats["events_tool_filtered"] += 1
                continue
            stats["after_event_type_filter"] += 1
            payload_text = str(event.get("payload_json", "{}") or "{}")
            source_exchange_id = str(event.get("source_exchange_id", "unknown") or "unknown").lower()
            try:
                payload = json.loads(payload_text)
            except Exception:
                payload = {}
                stats["events_json_error"] += 1

            if "@bookticker" in stream or "bookticker" in event_type:
                stats["events_bookticker"] += 1
                if isinstance(payload, dict):
                    b = float(payload.get("b") or payload.get("bidPrice") or 0.0)
                    a = float(payload.get("a") or payload.get("askPrice") or 0.0)
                    if b > 0:
                        best_bid = b
                    if a > 0:
                        best_ask = a
                    if b > 0 and a > 0:
                        last_book_ts_ms = int(ts_ms)
                    if b > 0 and a > 0 and a >= b:
                        candidate_mid = float((b + a) / 2.0)
                        bookticker_updates_total += 1
                        if last_label_price > 0:
                            jump = abs((candidate_mid / last_label_price) - 1.0)
                            if jump > 0.01:
                                reset_gap_ms = int(ts_ms - last_label_price_ts_ms) if last_label_price_ts_ms > 0 else 0
                                # Accept large jumps when there has been a long gap since the last accepted label
                                # price; this avoids permanently rejecting a new regime after recorder gaps.
                                if reset_gap_ms >= 120_000:
                                    last_label_price = candidate_mid
                                    last_label_price_ts_ms = int(ts_ms)
                                    label_price_reset_accept_count += 1
                                else:
                                    label_price_rejected_count += 1
                                    if len(label_price_rejection_samples) < 20:
                                        label_price_rejection_samples.append(
                                            {
                                                "ts_ms": int(ts_ms),
                                                "prev_price": float(round(last_label_price, 8)),
                                                "candidate_price": float(round(candidate_mid, 8)),
                                                "jump_pct": float(round(jump * 100.0, 6)),
                                                "reset_gap_ms": int(reset_gap_ms),
                                            }
                                        )
                            else:
                                last_label_price = candidate_mid
                                last_label_price_ts_ms = int(ts_ms)
                        else:
                            last_label_price = candidate_mid
                            last_label_price_ts_ms = int(ts_ms)
            elif "@depth" in stream or "external_depth" in event_type:
                stats["events_depth"] += 1
                if isinstance(payload, dict):
                    last_depth_ts_ms = int(ts_ms)
                    bids = self._parse_depth_levels(payload.get("b") or payload.get("bids") or [])
                    asks = self._parse_depth_levels(payload.get("a") or payload.get("asks") or [])
                    cur_bids_map = {float(px): float(qty) for px, qty in bids if px > 0 and qty >= 0}
                    cur_asks_map = {float(px): float(qty) for px, qty in asks if px > 0 and qty >= 0}
                    canceled_qty = 0.0
                    added_qty = 0.0
                    if prev_depth_bids:
                        for px, prev_qty in prev_depth_bids.items():
                            cur_qty = cur_bids_map.get(px)
                            if cur_qty is None:
                                canceled_qty += max(0.0, prev_qty)
                            elif cur_qty < prev_qty:
                                canceled_qty += max(0.0, prev_qty - cur_qty)
                            elif cur_qty > prev_qty:
                                added_qty += max(0.0, cur_qty - prev_qty)
                    if prev_depth_asks:
                        for px, prev_qty in prev_depth_asks.items():
                            cur_qty = cur_asks_map.get(px)
                            if cur_qty is None:
                                canceled_qty += max(0.0, prev_qty)
                            elif cur_qty < prev_qty:
                                canceled_qty += max(0.0, prev_qty - cur_qty)
                            elif cur_qty > prev_qty:
                                added_qty += max(0.0, cur_qty - prev_qty)
                    cancel_proxy_window.append((ts_ms, canceled_qty))
                    passive_add_window.append((ts_ms, added_qty))
                    cancel_proxy_sum += canceled_qty
                    passive_add_sum += added_qty
                    prev_depth_bids = cur_bids_map
                    prev_depth_asks = cur_asks_map
                    if bids:
                        best_bid = float(bids[0][0])
                        bid_depth_top10 = float(sum(level[1] for level in bids))
                        strongest_bid_price = max(bids, key=lambda item: item[1])[0]
                        if best_bid > 0:
                            nearest_bid_wall_distance_pct = abs(best_bid - strongest_bid_price) / best_bid * 100.0
                    if asks:
                        best_ask = float(asks[0][0])
                        ask_depth_top10 = float(sum(level[1] for level in asks))
                        strongest_ask_price = max(asks, key=lambda item: item[1])[0]
                        if best_ask > 0:
                            nearest_ask_wall_distance_pct = abs(strongest_ask_price - best_ask) / best_ask * 100.0
            elif (
                "@aggtrade" in stream
                or "@trade" in stream
                or "trade_batch" in event_type
                or "aggtrade" in event_type
                or event_type == "trade"
            ):
                stats["events_trade"] += 1
                trade_rows: list[dict[str, Any]] = []
                if isinstance(payload, dict) and isinstance(payload.get("trades"), list):
                    trade_rows = [item for item in payload.get("trades", []) if isinstance(item, dict)]
                elif isinstance(payload, dict):
                    trade_rows = [payload]
                for trade in trade_rows:
                    t_ts = int(trade.get("ts_ms") or trade.get("T") or trade.get("t") or ts_ms or 0)
                    price = float(trade.get("price") or trade.get("p") or 0.0)
                    qty = float(trade.get("qty") or trade.get("q") or trade.get("amount") or 0.0)
                    side = str(trade.get("side") or "").upper()
                    if side not in {"BUY", "SELL"}:
                        maker = trade.get("m")
                        side = "SELL" if maker is True else "BUY"
                    if price > 0:
                        last_mid = price
                    if t_ts <= 0 or qty <= 0:
                        continue
                    last_trade_ts_ms = int(t_ts)
                    trade_ts.append(t_ts)
                    notional = max(0.0, price * qty)
                    notional_window.append((t_ts, notional))
                    qty_window.append((t_ts, qty))
                    qty_window_30.append((t_ts, qty))
                    notional_sum += notional
                    qty_sum += qty
                    qty_sum_30 += qty
                    if side == "BUY":
                        buy_qty_window.append((t_ts, qty))
                        buy_qty_sum += qty
                    else:
                        sell_qty_window.append((t_ts, qty))
                        sell_qty_sum += qty
            elif "@markprice" in stream or "mark_funding" in event_type:
                stats["events_mark"] += 1
                if isinstance(payload, dict):
                    mark = float(payload.get("p") or payload.get("markPrice") or payload.get("mark_price") or 0.0)
                    if mark > 0:
                        last_mid = mark
                    funding_rate = float(payload.get("r") or payload.get("fundingRate") or payload.get("funding_rate") or funding_rate)
                    funding_history.append((ts_ms, funding_rate))
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
                        open_interest = oi_value
                        oi_history.append((ts_ms, oi_value))
            elif "@forceorder" in stream or "liquidation" in event_type:
                stats["events_liq"] += 1
                if isinstance(payload, dict) and isinstance(payload.get("liquidations"), list):
                    for liq in payload.get("liquidations", []):
                        if not isinstance(liq, dict):
                            continue
                        l_ts = int(liq.get("ts_ms") or ts_ms or 0)
                        l_price = float(liq.get("price") or liq.get("p") or liq.get("avg_price") or last_mid or 0.0)
                        l_qty = float(liq.get("qty") or liq.get("q") or liq.get("amount") or 0.0)
                        l_side = str(liq.get("side") or liq.get("S") or "").upper()
                        if l_side in {"LONG", "BUY"}:
                            l_side = "BUY"
                        elif l_side in {"SHORT", "SELL"}:
                            l_side = "SELL"
                        if l_ts > 0:
                            liq_ts.append(l_ts)
                            l_notional = max(0.0, l_price * l_qty)
                            liq_notional_60_window.append((l_ts, l_notional))
                            liq_notional_10_window.append((l_ts, l_notional))
                            liq_notional_60_sum += l_notional
                            liq_notional_10_sum += l_notional
                            if l_side == "BUY":
                                liq_buy_notional_60_window.append((l_ts, l_notional))
                                liq_buy_notional_60_sum += l_notional
                            elif l_side == "SELL":
                                liq_sell_notional_60_window.append((l_ts, l_notional))
                                liq_sell_notional_60_sum += l_notional
                elif isinstance(payload, dict):
                    l_ts = int(payload.get("ts_ms") or ts_ms or 0)
                    l_notional = 0.0
                    l_side = ""
                    force = payload.get("o")
                    if isinstance(force, dict):
                        l_ts = int(force.get("T") or force.get("t") or l_ts or 0)
                        l_price = float(force.get("ap") or force.get("p") or last_mid or 0.0)
                        l_qty = float(force.get("q") or force.get("qty") or 0.0)
                        l_notional = max(0.0, l_price * l_qty)
                        l_side = str(force.get("S") or "").upper()
                    else:
                        l_price = float(payload.get("price") or payload.get("p") or last_mid or 0.0)
                        l_qty = float(payload.get("qty") or payload.get("q") or payload.get("amount") or 0.0)
                        l_notional = max(0.0, l_price * l_qty)
                        l_side = str(payload.get("side") or payload.get("S") or "").upper()
                    if l_side in {"LONG", "BUY"}:
                        l_side = "BUY"
                    elif l_side in {"SHORT", "SELL"}:
                        l_side = "SELL"
                    if l_ts > 0:
                        liq_ts.append(l_ts)
                        liq_notional_60_window.append((l_ts, l_notional))
                        liq_notional_10_window.append((l_ts, l_notional))
                        liq_notional_60_sum += l_notional
                        liq_notional_10_sum += l_notional
                        if l_side == "BUY":
                            liq_buy_notional_60_window.append((l_ts, l_notional))
                            liq_buy_notional_60_sum += l_notional
                        elif l_side == "SELL":
                            liq_sell_notional_60_window.append((l_ts, l_notional))
                            liq_sell_notional_60_sum += l_notional

            book_mid = 0.0
            if best_bid > 0 and best_ask > 0 and best_ask >= best_bid:
                last_spread = best_ask - best_bid
                last_mid = (best_bid + best_ask) / 2.0
                book_mid = float(last_mid)
                spread_history.append((ts_ms, last_spread))

            cutoff_trade = ts_ms - trade_window_ms
            cutoff_cancel = ts_ms - 10_000
            while trade_ts and trade_ts[0] < cutoff_trade:
                trade_ts.popleft()
            while buy_qty_window and buy_qty_window[0][0] < cutoff_trade:
                buy_qty_sum -= float(buy_qty_window[0][1])
                buy_qty_window.popleft()
            while sell_qty_window and sell_qty_window[0][0] < cutoff_trade:
                sell_qty_sum -= float(sell_qty_window[0][1])
                sell_qty_window.popleft()
            while notional_window and notional_window[0][0] < cutoff_trade:
                notional_sum -= float(notional_window[0][1])
                notional_window.popleft()
            while qty_window and qty_window[0][0] < cutoff_trade:
                qty_sum -= float(qty_window[0][1])
                qty_window.popleft()
            cutoff_trade_30 = ts_ms - 30_000
            while qty_window_30 and qty_window_30[0][0] < cutoff_trade_30:
                qty_sum_30 -= float(qty_window_30[0][1])
                qty_window_30.popleft()
            while cancel_proxy_window and cancel_proxy_window[0][0] < cutoff_cancel:
                cancel_proxy_sum -= float(cancel_proxy_window[0][1])
                cancel_proxy_window.popleft()
            while passive_add_window and passive_add_window[0][0] < cutoff_cancel:
                passive_add_sum -= float(passive_add_window[0][1])
                passive_add_window.popleft()
            cutoff_liq = ts_ms - liq_window_ms
            while liq_ts and liq_ts[0] < cutoff_liq:
                liq_ts.popleft()
            while liq_notional_60_window and liq_notional_60_window[0][0] < cutoff_liq:
                liq_notional_60_sum -= float(liq_notional_60_window[0][1]); liq_notional_60_window.popleft()
            while liq_buy_notional_60_window and liq_buy_notional_60_window[0][0] < cutoff_liq:
                liq_buy_notional_60_sum -= float(liq_buy_notional_60_window[0][1]); liq_buy_notional_60_window.popleft()
            while liq_sell_notional_60_window and liq_sell_notional_60_window[0][0] < cutoff_liq:
                liq_sell_notional_60_sum -= float(liq_sell_notional_60_window[0][1]); liq_sell_notional_60_window.popleft()
            cutoff_liq_10 = ts_ms - 10_000
            while liq_notional_10_window and liq_notional_10_window[0][0] < cutoff_liq_10:
                liq_notional_10_sum -= float(liq_notional_10_window[0][1]); liq_notional_10_window.popleft()
            while mid_history and mid_history[0][0] < (ts_ms - 61_000):
                mid_history.popleft()
            while oi_history and oi_history[0][0] < (ts_ms - 61_000):
                oi_history.popleft()
            while spread_history and spread_history[0][0] < (ts_ms - 31_000):
                spread_history.popleft()
            while funding_history and funding_history[0][0] < (ts_ms - 61_000):
                funding_history.popleft()

            if last_mid <= 0 and last_label_price <= 0:
                stats["skip_no_mid"] += 1
                continue
            if last_label_price > 0:
                label_price = float(last_label_price)
                label_price_direct_count += 1
            else:
                stats["skip_no_mid"] += 1
                continue
            mid_history.append((ts_ms, float(last_mid)))
            if grid_next_ts <= 0:
                grid_next_ts = int((ts_ms // sample_ms) * sample_ms)
            gap_ms = int(ts_ms - grid_next_ts)
            if gap_ms > max_grid_gap_ms:
                skipped_rows = max(0, int(gap_ms // sample_ms) - 1)
                stats["grid_gap_resync_count"] += 1
                stats["grid_gap_rows_skipped_estimate"] += int(skipped_rows)
                grid_next_ts = int((ts_ms // sample_ms) * sample_ms)
            if ts_ms < grid_next_ts:
                continue

            total_depth = max(1e-9, bid_depth_top10 + ask_depth_top10)
            bid_depth_rel = bid_depth_top10 / total_depth
            ask_depth_rel = ask_depth_top10 / total_depth
            imbalance = (bid_depth_top10 - ask_depth_top10) / total_depth
            total_trade_qty = max(1e-12, buy_qty_sum + sell_qty_sum)
            ladder_buy_pct = (buy_qty_sum / total_trade_qty) * 100.0 if total_trade_qty > 0 else 0.0
            ladder_sell_pct = (sell_qty_sum / total_trade_qty) * 100.0 if total_trade_qty > 0 else 0.0
            vwap_10s = (notional_sum / qty_sum) if qty_sum > 0 else last_mid

            mid_15 = None
            mid_60 = None
            for hist_ts, hist_mid in reversed(mid_history):
                age = ts_ms - hist_ts
                if mid_15 is None and age >= 15_000:
                    mid_15 = hist_mid
                if mid_60 is None and age >= 60_000:
                    mid_60 = hist_mid
                    break
            ret_15s = ((last_mid - mid_15) / mid_15) if mid_15 and mid_15 > 0 else 0.0
            ret_60s = ((last_mid - mid_60) / mid_60) if mid_60 and mid_60 > 0 else 0.0
            mid_5 = None
            mid_30 = None
            for hist_ts, hist_mid in reversed(mid_history):
                age = ts_ms - hist_ts
                if mid_5 is None and age >= 5_000:
                    mid_5 = hist_mid
                if mid_30 is None and age >= 30_000:
                    mid_30 = hist_mid
                if mid_5 is not None and mid_30 is not None:
                    break
            ret_5s = ((last_mid - mid_5) / mid_5) if mid_5 and mid_5 > 0 else 0.0
            ret_30s = ((last_mid - mid_30) / mid_30) if mid_30 and mid_30 > 0 else 0.0

            rets_30s: list[float] = []
            prev_mid = None
            for hist_ts, hist_mid in mid_history:
                if hist_ts < ts_ms - 30_000:
                    continue
                if prev_mid and prev_mid > 0 and hist_mid > 0:
                    rets_30s.append((hist_mid - prev_mid) / prev_mid)
                prev_mid = hist_mid
            if len(rets_30s) >= 2:
                mean = sum(rets_30s) / len(rets_30s)
                var = sum((r - mean) ** 2 for r in rets_30s) / max(1, len(rets_30s) - 1)
                rolling_vol_30s = var ** 0.5
            else:
                rolling_vol_30s = 0.0
            rets_60s: list[float] = []
            prev_mid_60 = None
            for hist_ts, hist_mid in mid_history:
                if hist_ts < ts_ms - 60_000:
                    continue
                if prev_mid_60 and prev_mid_60 > 0 and hist_mid > 0:
                    rets_60s.append((hist_mid - prev_mid_60) / prev_mid_60)
                prev_mid_60 = hist_mid
            if len(rets_60s) >= 2:
                mean_60 = sum(rets_60s) / len(rets_60s)
                var_60 = sum((r - mean_60) ** 2 for r in rets_60s) / max(1, len(rets_60s) - 1)
                rolling_vol_60s = var_60 ** 0.5
            else:
                rolling_vol_60s = 0.0

            oi_ref = None
            for oi_ts, oi_val in reversed(oi_history):
                if ts_ms - oi_ts >= 60_000:
                    oi_ref = oi_val
                    break
            oi_change_pct = ((open_interest - oi_ref) / oi_ref) if oi_ref and oi_ref > 0 else 0.0
            volume_10s = max(0.0, buy_qty_sum + sell_qty_sum)
            volume_30s = max(0.0, qty_sum_30)
            relative_volume_ratio = (volume_10s / (volume_30s / 3.0)) if volume_30s > 0 else 0.0
            aggressive_delta = self._aggressive_buy_sell_delta(buy_qty_sum, sell_qty_sum)
            liq_count_60 = float(len(liq_ts))
            liq_notional_60 = float(max(0.0, liq_notional_60_sum))
            liq_imbalance = self._safe_div(
                float(liq_buy_notional_60_sum - liq_sell_notional_60_sum),
                float(max(1e-12, liq_notional_60_sum)),
            )
            liq_momentum = self._safe_div(float(liq_notional_10_sum), float(max(1e-12, liq_notional_60_sum / 6.0))) - 1.0
            spread_ref = None
            for s_ts, s_val in reversed(spread_history):
                if ts_ms - s_ts >= 5_000:
                    spread_ref = s_val
                    break
            spread_change_rate = ((last_spread - spread_ref) / spread_ref) if spread_ref and spread_ref > 0 else 0.0
            ema_fast = last_mid
            ema_slow = last_mid
            alpha_fast = 2.0 / 9.0
            alpha_slow = 2.0 / 22.0
            for hist_ts, hist_mid in mid_history:
                if hist_ts < ts_ms - 60_000:
                    continue
                ema_fast = alpha_fast * hist_mid + (1.0 - alpha_fast) * ema_fast
                ema_slow = alpha_slow * hist_mid + (1.0 - alpha_slow) * ema_slow
            ema_fast_distance = ((last_mid - ema_fast) / ema_fast) if ema_fast > 0 else 0.0
            ema_slow_distance = ((last_mid - ema_slow) / ema_slow) if ema_slow > 0 else 0.0
            trend_anchor = None
            for hist_ts, hist_mid in reversed(mid_history):
                if ts_ms - hist_ts >= 20_000:
                    trend_anchor = hist_mid
                    break
            trend_slope_short = ((last_mid - trend_anchor) / trend_anchor) if trend_anchor and trend_anchor > 0 else 0.0
            wall_strength_bid = bid_depth_rel
            wall_strength_ask = ask_depth_rel
            dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            hour_float = dt.hour + (dt.minute / 60.0) + (dt.second / 3600.0)
            hour_angle = (hour_float / 24.0) * (2.0 * math.pi)
            hour_sin = math.sin(hour_angle)
            hour_cos = math.cos(hour_angle)
            if dt.hour < 8:
                session_code = 0.0  # Asia
            elif dt.hour < 16:
                session_code = 1.0  # EU
            else:
                session_code = 2.0  # US
            oi_prev = None
            for o_ts, o_val in reversed(oi_history):
                if ts_ms - o_ts >= 10_000:
                    oi_prev = o_val
                    break
            oi_velocity = ((open_interest - oi_prev) / oi_prev) if oi_prev and oi_prev > 0 else 0.0
            funding_prev = None
            for f_ts, f_val in reversed(funding_history):
                if ts_ms - f_ts >= 60_000:
                    funding_prev = f_val
                    break
            funding_rate_change = (funding_rate - funding_prev) if funding_prev is not None else 0.0

            while grid_next_ts <= ts_ms:
                stats["grid_rows_attempted"] += 1
                book_age_ms = int(max(0, grid_next_ts - last_book_ts_ms)) if last_book_ts_ms > 0 else 10**9
                trade_age_ms = int(max(0, grid_next_ts - last_trade_ts_ms)) if last_trade_ts_ms > 0 else 10**9
                depth_age_ms = int(max(0, grid_next_ts - last_depth_ts_ms)) if last_depth_ts_ms > 0 else 10**9
                is_stale_book = bool(book_age_ms > stale_book_ms)
                is_stale_trade = bool(trade_age_ms > stale_trade_ms)
                is_stale_depth = bool(depth_age_ms > stale_depth_ms)
                if not is_stale_book:
                    stats["rows_with_valid_book"] += 1
                else:
                    stats["rows_rejected_stale_book"] += 1
                if not is_stale_trade:
                    stats["rows_with_recent_trade"] += 1
                else:
                    stats["rows_rejected_stale_trade"] += 1
                if last_spread > 0:
                    stats["rows_with_valid_spread"] += 1
                else:
                    stats["rows_rejected_spread"] += 1
                if label_price <= 0:
                    stats["rows_rejected_missing_price"] += 1
                is_stale_row = bool(is_stale_book or is_stale_depth)
                if is_stale_row:
                    stats["stale_row_count"] += 1
                rows.append(
                    {
                    "ts_ms": int(grid_next_ts),
                    "source_exchange_id": source_exchange_id,
                    "enabled_exchange_ids": [source_exchange_id],
                    "spread": float(last_spread),
                    "mid_price": float(last_mid),
                    "label_price": float(label_price),
                    "book_bid_depth": float(bid_depth_rel),
                    "book_ask_depth": float(ask_depth_rel),
                    "book_imbalance_top10": float(imbalance),
                    "top10_bid_volume": float(bid_depth_rel),
                    "top10_ask_volume": float(ask_depth_rel),
                    "trade_rate_10s": float(len(trade_ts)),
                    "liq_events_60s": float(len(liq_ts)),
                    "liq_count_60s": float(liq_count_60),
                    "liq_notional_60s": float(liq_notional_60),
                    "liq_buy_sell_imbalance": float(liq_imbalance),
                    "liq_momentum": float(liq_momentum),
                    "funding_rate": float(funding_rate),
                    "ladder_buy_pct": float(ladder_buy_pct),
                    "ladder_sell_pct": float(ladder_sell_pct),
                    "nearest_bid_wall_distance_pct": float(nearest_bid_wall_distance_pct),
                    "nearest_ask_wall_distance_pct": float(nearest_ask_wall_distance_pct),
                    "return_15s": float(ret_15s),
                    "return_60s": float(ret_60s),
                    "rolling_volatility_30s": float(rolling_vol_30s),
                    "vwap_distance_pct": float(((last_mid - vwap_10s) / vwap_10s) if vwap_10s > 0 else 0.0),
                    "market_buy_volume_10s": float(max(0.0, buy_qty_sum)),
                    "market_sell_volume_10s": float(max(0.0, sell_qty_sum)),
                    "open_interest_change_pct": float(oi_change_pct),
                    "return_5s": float(ret_5s),
                    "return_30s": float(ret_30s),
                    "rolling_volatility_60s": float(rolling_vol_60s),
                    "candle_range_pct": 0.0,
                    "atr_short": 0.0,
                    "ema_fast_distance_pct": float(ema_fast_distance),
                    "ema_slow_distance_pct": float(ema_slow_distance),
                    "trend_slope_short": float(trend_slope_short),
                    "aggressive_buy_sell_delta": float(aggressive_delta),
                    "cancel_rate_orderbook": float(
                        max(0.0, self._safe_div(cancel_proxy_sum, max(1e-9, cancel_proxy_sum + passive_add_sum)))
                    ),
                    "wall_strength_bid": float(wall_strength_bid),
                    "wall_strength_ask": float(wall_strength_ask),
                    "wall_persistence_seconds": 0.0,
                    "spread_change_rate": float(spread_change_rate),
                    "volume_10s": float(volume_10s),
                    "volume_30s": float(volume_30s),
                    "relative_volume_ratio": float(relative_volume_ratio),
                    "hour_of_day_sin": float(hour_sin),
                    "hour_of_day_cos": float(hour_cos),
                    "session_asia_eu_us": float(session_code),
                    "oi_velocity": float(oi_velocity),
                    "funding_rate_change": float(funding_rate_change),
                    "book_age_ms": int(book_age_ms),
                    "trade_age_ms": int(trade_age_ms),
                    "depth_age_ms": int(depth_age_ms),
                    "is_stale_book": bool(is_stale_book),
                    "is_stale_trade": bool(is_stale_trade),
                    "is_stale_depth": bool(is_stale_depth),
                    "is_stale_row": bool(is_stale_row),
                }
                )
                stats["rows_emitted"] += 1
                grid_next_ts += sample_ms
        setattr(self, "_last_market_feature_stats", stats)
        setattr(
            self,
            "_last_label_price_stats",
            {
                "source_used": "bookticker_mid_state",
                "direct_count": int(label_price_direct_count),
                "ffill_count": int(label_price_ffill_count),
                "ffill_rate": float(label_price_ffill_count / max(1, (label_price_direct_count + label_price_ffill_count))),
                "bookticker_updates_total": int(bookticker_updates_total),
                "rejected_count": int(label_price_rejected_count),
                "rejected_rate": float(label_price_rejected_count / max(1, bookticker_updates_total)),
                "reset_accept_count": int(label_price_reset_accept_count),
                "rejection_samples": label_price_rejection_samples,
            },
        )
        return rows

    def _market_events_to_feature_rows_streaming(
        self,
        events: list[dict[str, Any]],
        *,
        sample_ms_override: int | None = None,
        micro_batch_events: int = 50_000,
        overlap_events: int = 5_000,
        max_batch_seconds: int = 120,
        max_rss_gb: float | None = None,
        output_path: Path | None = None,
        min_ts_exclusive: int = -1,
        append_output: bool = False,
    ) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        """Convert market events to feature rows in micro-batches to bound memory.

        Returns rows plus aggregated feature/label stats.
        """
        if not events:
            return [], {}, {}
        micro = max(25_000, int(micro_batch_events))
        overlap = max(5_000, min(int(overlap_events), micro // 2))
        out_rows: list[dict[str, Any]] = []
        wrote_rows = 0
        wrote_ts_min = 0
        wrote_ts_max = 0
        total_stats: dict[str, int] = {}
        label_stats: dict[str, Any] = {}
        last_ts = int(min_ts_exclusive)
        batches_total = 0
        rows_dedup_dropped = 0
        n = len(events)
        start = 0
        out_handle = None
        if output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            out_handle = output_path.open("a" if append_output else "w", encoding="utf-8")
        while start < n:
            batch_started = time.time()
            end = min(n, start + micro)
            batch_start = max(0, start - overlap) if start > 0 else 0
            batch = events[batch_start:end]
            rows = self._market_events_to_feature_rows(batch, sample_ms_override=sample_ms_override)
            elapsed = time.time() - batch_started
            if elapsed > float(max_batch_seconds):
                raise RuntimeError(
                    f"Feature conversion batch timeout: elapsed={elapsed:.1f}s limit={max_batch_seconds}s "
                    f"batch_events={len(batch)} range={batch_start}:{end}"
                )
            if max_rss_gb is not None and max_rss_gb > 0:
                rss_now = self._process_resident_memory_bytes()
                if rss_now is None:
                    rss_now = self._process_tree_resident_memory_bytes() or 0
                if rss_now >= int(max_rss_gb * (1024**3)):
                    raise RuntimeError(
                        f"Feature conversion exceeded RAM guard: rss_gb={rss_now / float(1024**3):.3f} "
                        f"limit_gb={max_rss_gb:.3f}"
                    )
            batch_stats = getattr(self, "_last_market_feature_stats", None)
            if isinstance(batch_stats, dict):
                for k, v in batch_stats.items():
                    if isinstance(v, (int, float)):
                        total_stats[k] = int(total_stats.get(k, 0) + int(v))
            ls = getattr(self, "_last_label_price_stats", None)
            if isinstance(ls, dict):
                label_stats = ls
            for row in rows:
                ts = int(row.get("ts_ms", 0) or 0)
                if ts <= last_ts:
                    rows_dedup_dropped += 1
                    continue
                if out_handle is None:
                    out_rows.append(row)
                else:
                    out_handle.write(json.dumps(row, separators=(",", ":"), ensure_ascii=True))
                    out_handle.write("\n")
                    wrote_rows += 1
                    if wrote_ts_min <= 0:
                        wrote_ts_min = ts
                    wrote_ts_max = ts
                last_ts = ts
            batches_total += 1
            del rows
            del batch
            gc.collect()
            start = end
        if out_handle is not None:
            out_handle.close()
        total_stats["streaming_batches_total"] = int(batches_total)
        total_stats["streaming_dedup_dropped"] = int(rows_dedup_dropped)
        total_stats["streaming_input_events"] = int(n)
        total_stats["streaming_output_rows"] = int(wrote_rows if out_handle is not None else len(out_rows))
        total_stats["streaming_written_rows"] = int(wrote_rows)
        total_stats["streaming_written_ts_min"] = int(wrote_ts_min)
        total_stats["streaming_written_ts_max"] = int(wrote_ts_max)
        return out_rows, total_stats, label_stats

    async def _build_market_event_replay_frames(
        self,
        *,
        pair_symbol: str,
        ts_start_ms: int | None = None,
        ts_end_ms: int | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if pq is None:
            return [], {}
        batch_ms = int(os.getenv("REPLAY_MARKET_BATCH_MS", "25"))
        batch_ms = max(20, min(50, batch_ms))
        max_events = max(20_000, int(os.getenv("REPLAY_MARKET_MAX_EVENTS", "250000")))
        chunks = self._list_market_event_chunks(
            pair_symbol=pair_symbol,
            ts_start_ms=ts_start_ms,
            ts_end_ms=ts_end_ms,
            limit=5000,
        )
        if not chunks:
            return [], {}
        events: list[dict[str, Any]] = []
        for chunk in chunks:
            path = Path(str(chunk.get("file_path", "")))
            if not path.exists():
                continue
            try:
                table = pq.ParquetFile(str(path)).read(
                    columns=[
                        "stream",
                        "event_type",
                        "ts_exchange_ms",
                        "ts_receive_ms",
                        "payload_json",
                    ],
                )
            except Exception:
                continue
            streams = table.column("stream").to_pylist()
            event_types = table.column("event_type").to_pylist()
            ts_exchange = table.column("ts_exchange_ms").to_pylist()
            ts_receive = table.column("ts_receive_ms").to_pylist()
            payload_json = table.column("payload_json").to_pylist()
            for idx in range(len(streams)):
                receive_ms = int(ts_receive[idx] or 0)
                exchange_ms = int(ts_exchange[idx] or 0)
                stream_value = str(streams[idx] or "")
                event_type_value = str(event_types[idx] or "")
                tool_id = event_stream_to_tool_id(stream_value, event_type_value)
                if tool_id and (not self._mode_tool_enabled("replay", tool_id)):
                    continue
                events.append(
                    {
                        "stream": stream_value,
                        "event_type": event_type_value,
                        "ts_receive_ms": receive_ms,
                        "ts_exchange_ms": exchange_ms,
                        "payload_json": str(payload_json[idx] or "{}"),
                    }
                )
        if not events:
            return [], {}
        events.sort(key=lambda item: (int(item.get("ts_receive_ms", 0) or 0), int(item.get("ts_exchange_ms", 0) or 0)))
        if len(events) > max_events:
            events = events[-max_events:]

        frames: list[dict[str, Any]] = []
        mid_hist: deque[tuple[float, float]] = deque(maxlen=20_000)
        spread_hist: deque[tuple[float, float]] = deque(maxlen=20_000)
        buy_hist: deque[tuple[float, float]] = deque(maxlen=20_000)
        sell_hist: deque[tuple[float, float]] = deque(maxlen=20_000)
        best_bid = 0.0
        best_ask = 0.0
        mark_price = 0.0
        funding_rate = 0.0
        last_trade_price = 0.0
        last_trade_qty = 0.0
        last_trade_side = ""
        last_liq_side = ""
        last_liq_qty = 0.0
        depth_levels_bids = 0
        depth_levels_asks = 0
        latest_bids: list[tuple[float, float]] = []
        latest_asks: list[tuple[float, float]] = []
        book_bids_map: dict[float, float] = {}
        book_asks_map: dict[float, float] = {}
        trade_ts: deque[int] = deque()
        liq_ts: deque[int] = deque()
        cancel_proxy_window: deque[tuple[int, float]] = deque()
        passive_add_window: deque[tuple[int, float]] = deque()
        cancel_proxy_sum = 0.0
        passive_add_sum = 0.0
        stream_counts_total: dict[str, int] = defaultdict(int)
        cumulative_events = 0

        def _payload(raw: str) -> Any:
            try:
                return json.loads(raw)
            except Exception:
                return {}

        def _parse_levels(raw: Any, max_levels: int = 160) -> list[tuple[float, float]]:
            out: list[tuple[float, float]] = []
            if not isinstance(raw, list):
                return out
            for level in raw:
                if not isinstance(level, (list, tuple)) or len(level) < 2:
                    continue
                try:
                    px = float(level[0] or 0.0)
                    qty = float(level[1] or 0.0)
                except Exception:
                    continue
                if px <= 0 or qty <= 0:
                    continue
                out.append((px, qty))
                if len(out) >= max_levels:
                    break
            return out

        def _infer_bucket_size(mid_price: float, spread_value: float | None) -> float:
            if not np.isfinite(mid_price) or mid_price <= 0:
                return 0.0001
            min_tick = 0.0
            for levels in (latest_bids, latest_asks):
                if len(levels) < 2:
                    continue
                for idx in range(1, len(levels)):
                    prev_px = float(levels[idx - 1][0])
                    cur_px = float(levels[idx][0])
                    diff = abs(prev_px - cur_px)
                    if diff <= 0:
                        continue
                    if min_tick <= 0 or diff < min_tick:
                        min_tick = diff
            if min_tick > 0:
                return max(0.0001, min_tick)
            if spread_value is not None and np.isfinite(spread_value) and spread_value > 0:
                # Keep enough bins inside spread to approximate live ladder granularity.
                return max(0.0001, float(spread_value) / 40.0)
            return max(0.0001, float(mid_price) * 0.00008)

        def _build_ladder_rows(mid_price: float, bucket_size: float, span_rows: int) -> list[dict[str, Any]]:
            if not np.isfinite(mid_price) or mid_price <= 0 or bucket_size <= 0:
                return []
            qty_by_row: dict[int, float] = {}
            for px, qty in latest_bids:
                row_idx = int(round((px - mid_price) / bucket_size))
                qty_by_row[row_idx] = float(qty_by_row.get(row_idx, 0.0)) + float(qty)
            for px, qty in latest_asks:
                row_idx = int(round((px - mid_price) / bucket_size))
                qty_by_row[row_idx] = float(qty_by_row.get(row_idx, 0.0)) + float(qty)
            rows: list[dict[str, Any]] = []
            for row_idx in range(-span_rows, span_rows + 1):
                price = mid_price + row_idx * bucket_size
                qty = float(qty_by_row.get(row_idx, 0.0))
                rows.append(
                    {
                        "row": int(row_idx),
                        "price": float(price),
                        "liquidity": float(max(0.0, qty)),
                        "side": "at" if row_idx == 0 else ("above" if row_idx > 0 else "below"),
                    }
                )
            return rows

        def _flush_batch(batch_ts_ms: int, batch_items: list[dict[str, Any]]) -> None:
            nonlocal best_bid, best_ask, mark_price, funding_rate
            nonlocal last_trade_price, last_trade_qty, last_trade_side
            nonlocal last_liq_side, last_liq_qty, depth_levels_bids, depth_levels_asks
            nonlocal latest_bids, latest_asks
            nonlocal cumulative_events
            if not batch_items:
                return
            batch_stream_counts: dict[str, int] = defaultdict(int)
            for item in batch_items:
                stream = str(item.get("stream", "") or "")
                stream_l = stream.lower()
                event_type = str(item.get("event_type", "") or "").lower()
                payload = _payload(str(item.get("payload_json", "{}")))
                event_ts = int(item.get("ts_receive_ms", 0) or item.get("ts_exchange_ms", 0) or batch_ts_ms)
                batch_stream_counts[stream] += 1
                stream_counts_total[stream] = int(stream_counts_total.get(stream, 0)) + 1

                if "@bookticker" in stream_l or "bookticker" in event_type:
                    if isinstance(payload, dict):
                        b = float(payload.get("b") or payload.get("bidPrice") or 0.0)
                        a = float(payload.get("a") or payload.get("askPrice") or 0.0)
                        if b > 0:
                            best_bid = b
                        if a > 0:
                            best_ask = a
                elif "@depth" in stream_l or "external_depth" in event_type:
                    if isinstance(payload, dict):
                        raw_bids = payload.get("b") or payload.get("bids") or []
                        raw_asks = payload.get("a") or payload.get("asks") or []
                        if isinstance(raw_bids, list):
                            for level in raw_bids:
                                if not isinstance(level, (list, tuple)) or len(level) < 2:
                                    continue
                                try:
                                    px = float(level[0] or 0.0)
                                    qty = float(level[1] or 0.0)
                                except Exception:
                                    continue
                                if px <= 0:
                                    continue
                                if qty <= 0:
                                    book_bids_map.pop(px, None)
                                else:
                                    book_bids_map[px] = qty
                        if isinstance(raw_asks, list):
                            for level in raw_asks:
                                if not isinstance(level, (list, tuple)) or len(level) < 2:
                                    continue
                                try:
                                    px = float(level[0] or 0.0)
                                    qty = float(level[1] or 0.0)
                                except Exception:
                                    continue
                                if px <= 0:
                                    continue
                                if qty <= 0:
                                    book_asks_map.pop(px, None)
                                else:
                                    book_asks_map[px] = qty

                        if book_bids_map:
                            latest_bids = sorted(book_bids_map.items(), key=lambda x: x[0], reverse=True)[:160]
                            best_bid = float(latest_bids[0][0])
                            depth_levels_bids = len(book_bids_map)
                        if book_asks_map:
                            latest_asks = sorted(book_asks_map.items(), key=lambda x: x[0])[:160]
                            best_ask = float(latest_asks[0][0])
                            depth_levels_asks = len(book_asks_map)
                        # Estimate cancel/add flow from per-level quantity deltas at unchanged prices.
                        canceled_qty = 0.0
                        added_qty = 0.0
                        for px, prev_qty in prev_depth_bids.items():
                            cur_qty = book_bids_map.get(px)
                            if cur_qty is None:
                                canceled_qty += max(0.0, prev_qty)
                            elif cur_qty < prev_qty:
                                canceled_qty += max(0.0, prev_qty - cur_qty)
                            elif cur_qty > prev_qty:
                                added_qty += max(0.0, cur_qty - prev_qty)
                        for px, prev_qty in prev_depth_asks.items():
                            cur_qty = book_asks_map.get(px)
                            if cur_qty is None:
                                canceled_qty += max(0.0, prev_qty)
                            elif cur_qty < prev_qty:
                                canceled_qty += max(0.0, prev_qty - cur_qty)
                            elif cur_qty > prev_qty:
                                added_qty += max(0.0, cur_qty - prev_qty)
                        cancel_proxy_window.append((batch_ts_ms, canceled_qty))
                        passive_add_window.append((batch_ts_ms, added_qty))
                        cancel_proxy_sum += canceled_qty
                        passive_add_sum += added_qty
                        prev_depth_bids = dict(book_bids_map)
                        prev_depth_asks = dict(book_asks_map)
                elif "@aggtrade" in stream_l or "trade_batch" in event_type or "aggtrade" in event_type:
                    trade_rows: list[dict[str, Any]] = []
                    if isinstance(payload, dict) and isinstance(payload.get("trades"), list):
                        trade_rows = [x for x in payload.get("trades", []) if isinstance(x, dict)]
                    elif isinstance(payload, dict):
                        trade_rows = [payload]
                    for trade in trade_rows:
                        t_ts = int(trade.get("ts_ms") or trade.get("T") or trade.get("t") or event_ts or 0)
                        price = float(trade.get("price") or trade.get("p") or 0.0)
                        qty = float(trade.get("qty") or trade.get("q") or trade.get("amount") or 0.0)
                        side = str(trade.get("side") or "").upper()
                        if side not in {"BUY", "SELL"}:
                            side = "SELL" if trade.get("m") is True else "BUY"
                        if price > 0:
                            last_trade_price = price
                        if qty > 0:
                            last_trade_qty = qty
                        if side:
                            last_trade_side = side
                        if t_ts > 0:
                            trade_ts.append(t_ts)
                            if qty > 0:
                                t_sec = float(t_ts) / 1000.0
                                if side == "BUY":
                                    buy_hist.append((t_sec, float(qty)))
                                elif side == "SELL":
                                    sell_hist.append((t_sec, float(qty)))
                elif "@markprice" in stream_l or "mark_funding" in event_type:
                    if isinstance(payload, dict):
                        mark = float(payload.get("p") or payload.get("markPrice") or payload.get("mark_price") or 0.0)
                        if mark > 0:
                            mark_price = mark
                        funding_rate = float(payload.get("r") or payload.get("fundingRate") or payload.get("funding_rate") or funding_rate)
                elif "@forceorder" in stream_l or "liquidation" in event_type:
                    if isinstance(payload, dict) and isinstance(payload.get("liquidations"), list):
                        for liq in payload.get("liquidations", []):
                            if not isinstance(liq, dict):
                                continue
                            l_ts = int(liq.get("ts_ms") or event_ts or 0)
                            qty = float(liq.get("qty") or liq.get("q") or 0.0)
                            side = str(liq.get("side") or liq.get("S") or "").upper()
                            if l_ts > 0:
                                liq_ts.append(l_ts)
                            if qty > 0:
                                last_liq_qty = qty
                            if side:
                                last_liq_side = side
                    elif isinstance(payload, dict):
                        force = payload.get("o")
                        if isinstance(force, dict):
                            l_ts = int(force.get("T") or force.get("t") or event_ts or 0)
                            qty = float(force.get("q") or force.get("qty") or 0.0)
                            side = str(force.get("S") or "").upper()
                            if l_ts > 0:
                                liq_ts.append(l_ts)
                            if qty > 0:
                                last_liq_qty = qty
                            if side:
                                last_liq_side = side
            cutoff_trade = batch_ts_ms - 10_000
            while trade_ts and trade_ts[0] < cutoff_trade:
                trade_ts.popleft()
            cutoff_liq = batch_ts_ms - 60_000
            while liq_ts and liq_ts[0] < cutoff_liq:
                liq_ts.popleft()
            cutoff_cancel = batch_ts_ms - 10_000
            while cancel_proxy_window and cancel_proxy_window[0][0] < cutoff_cancel:
                cancel_proxy_sum -= float(cancel_proxy_window[0][1])
                cancel_proxy_window.popleft()
            while passive_add_window and passive_add_window[0][0] < cutoff_cancel:
                passive_add_sum -= float(passive_add_window[0][1])
                passive_add_window.popleft()

            cumulative_events += len(batch_items)
            spread = (best_ask - best_bid) if best_ask > 0 and best_bid > 0 and best_ask >= best_bid else None
            mid_price = (
                float((best_bid + best_ask) / 2.0)
                if (best_bid > 0 and best_ask > 0 and best_ask >= best_bid)
                else (mark_price if mark_price > 0 else (last_trade_price if last_trade_price > 0 else 0.0))
            )
            now_s = float(batch_ts_ms) / 1000.0
            if mid_price > 0:
                mid_hist.append((now_s, float(mid_price)))
            if spread is not None and spread >= 0:
                spread_hist.append((now_s, float(spread)))
            while buy_hist and now_s - buy_hist[0][0] > 40.0:
                buy_hist.popleft()
            while sell_hist and now_s - sell_hist[0][0] > 40.0:
                sell_hist.popleft()

            def _ret(sec: float) -> float:
                if mid_price <= 0:
                    return 0.0
                ref = None
                for t, v in reversed(mid_hist):
                    if now_s - t >= sec:
                        ref = v
                        break
                return ((mid_price - ref) / ref) if (ref and ref > 0) else 0.0

            def _vol(sec: float) -> float:
                vals = [v for t, v in mid_hist if now_s - t <= sec and v > 0]
                if len(vals) < 2:
                    return 0.0
                arr = np.asarray(vals, dtype=np.float64)
                rets = np.diff(np.log(arr))
                return float(np.std(rets)) if rets.size else 0.0

            buy_10 = float(sum(q for t, q in buy_hist if now_s - t <= 10.0))
            sell_10 = float(sum(q for t, q in sell_hist if now_s - t <= 10.0))
            vol_10 = max(0.0, buy_10 + sell_10)
            aggressive_delta = ((buy_10 - sell_10) / vol_10) if vol_10 > 0 else 0.0
            vwap_num = 0.0
            vwap_den = 0.0
            for t, qty in list(buy_hist) + list(sell_hist):
                if now_s - t <= 10.0 and qty > 0 and mid_price > 0:
                    vwap_num += mid_price * qty
                    vwap_den += qty
            vwap_10 = (vwap_num / vwap_den) if vwap_den > 0 else mid_price
            vwap_distance_pct = ((mid_price - vwap_10) / vwap_10) if vwap_10 > 0 else 0.0
            spread_ref = None
            for t, v in reversed(spread_hist):
                if now_s - t >= 5.0:
                    spread_ref = v
                    break
            spread_change_rate = ((float(spread or 0.0) - spread_ref) / spread_ref) if (spread_ref and spread_ref > 0) else 0.0
            ret_5 = _ret(5.0)
            ret_15 = _ret(15.0)
            ret_30 = _ret(30.0)
            ret_60 = _ret(60.0)
            vol_30 = _vol(30.0)
            vol_60 = _vol(60.0)
            bucket_size = _infer_bucket_size(mid_price, spread)
            span_rows = 40
            if spread is not None and spread > 0 and bucket_size > 0:
                approx = int(np.ceil((spread / bucket_size) * 8))
                span_rows = max(24, min(80, approx))
            ladder_rows = _build_ladder_rows(mid_price, bucket_size, span_rows)
            heatmap_levels = []
            for row in ladder_rows:
                liq = float(row.get("liquidity", 0.0) or 0.0)
                if liq <= 0:
                    continue
                heatmap_levels.append(
                    {
                        "row": int(row.get("row", 0)),
                        "value": float(liq),
                    }
                )
            ladder_snapshot = {
                "type": "ladder_snapshot",
                "symbol": pair_symbol,
                "ts_ms": int(batch_ts_ms),
                "current_price": float(mid_price if mid_price > 0 else (mark_price if mark_price > 0 else last_trade_price)),
                "bucket_size": float(bucket_size),
                "rows": ladder_rows,
            }
            heatmap_column = {
                "ts_ms": int(batch_ts_ms),
                "center_row": 0,
                "row_min": -int(span_rows),
                "row_max": int(span_rows),
                "rows": heatmap_levels,
            }
            received_at = datetime.fromtimestamp(batch_ts_ms / 1000.0, tz=timezone.utc).isoformat()
            snapshot = {
                "type": "terminal_snapshot",
                "symbol": pair_symbol,
                "received_at": received_at,
                "event_count": int(cumulative_events),
                "book_synced": best_bid > 0 and best_ask > 0,
                "last_update_id": int(cumulative_events),
                "best_bid": best_bid if best_bid > 0 else None,
                "best_ask": best_ask if best_ask > 0 else None,
                "spread": spread,
                "mark_price": mark_price if mark_price > 0 else None,
                "funding_rate": funding_rate,
                "last_trade_price": last_trade_price if last_trade_price > 0 else None,
                "last_trade_qty": last_trade_qty if last_trade_qty > 0 else None,
                "last_trade_side": last_trade_side or None,
                "last_kline_close": None,
                "last_kline_interval": "1m",
                "last_liquidation_side": last_liq_side or None,
                "last_liquidation_qty": last_liq_qty if last_liq_qty > 0 else None,
                "trade_rate_10s": int(len(trade_ts)),
                "liq_events_60s": int(len(liq_ts)),
                "sync_failures": 0,
                "stream_counts": dict(batch_stream_counts),
                "depth_levels": {"bids": depth_levels_bids, "asks": depth_levels_asks},
                "decision_layer": {
                    "source_mode": "market_event_replay",
                    "status_note": "Market state rebuilt from recorded full-fidelity events.",
                    "market_ladder_snapshot": ladder_snapshot,
                    "market_heatmap_column": heatmap_column,
                    "market_heatmap_bucket_size": float(bucket_size),
                    "market_heatmap_ticks_per_bucket": 2,
                    "market_heatmap_time_bucket_ms": int(batch_ms),
                    "market_heatmap_max_columns": 14_400,
                },
                "return_5s": float(ret_5),
                "return_15s": float(ret_15),
                "return_30s": float(ret_30),
                "return_60s": float(ret_60),
                "rolling_volatility_30s": float(vol_30),
                "rolling_volatility_60s": float(vol_60),
                "candle_range_pct": 0.0,
                "atr_short": 0.0,
                "ema_fast_distance_pct": 0.0,
                "ema_slow_distance_pct": 0.0,
                "trend_slope_short": 0.0,
                "vwap_distance_pct": float(vwap_distance_pct),
                "market_buy_volume_10s": float(buy_10),
                "market_sell_volume_10s": float(sell_10),
                "aggressive_buy_sell_delta": float(aggressive_delta),
                "wall_strength_bid": float(
                    (
                        sum(float(q) for _, q in latest_bids[:10])
                        / max(
                            1e-9,
                            (
                                sum(float(q) for _, q in latest_bids[:10])
                                + sum(float(q) for _, q in latest_asks[:10])
                            ),
                        )
                    )
                ) if (latest_bids or latest_asks) else 0.0,
                "wall_strength_ask": float(
                    (
                        sum(float(q) for _, q in latest_asks[:10])
                        / max(
                            1e-9,
                            (
                                sum(float(q) for _, q in latest_bids[:10])
                                + sum(float(q) for _, q in latest_asks[:10])
                            ),
                        )
                    )
                ) if (latest_bids or latest_asks) else 0.0,
                "spread_change_rate": float(spread_change_rate),
                "cancel_rate_orderbook": float(
                    max(0.0, self._safe_div(cancel_proxy_sum, max(1e-9, cancel_proxy_sum + passive_add_sum)))
                ),
                "session_asia_eu_us": 2.0,
                "open_interest_change_pct": 0.0,
                "oi_velocity": 0.0,
                "funding_rate_change": 0.0,
            }
            frames.append(
                {
                    "ts_ms": int(batch_ts_ms),
                    "processed_events": int(cumulative_events),
                    "events_in_batch": int(len(batch_items)),
                    "terminal_snapshot": snapshot,
                }
            )

        current_bucket: int | None = None
        batch: list[dict[str, Any]] = []
        for event in events:
            ts_ms = int(event.get("ts_receive_ms", 0) or event.get("ts_exchange_ms", 0) or 0)
            if ts_ms <= 0:
                continue
            bucket = (ts_ms // batch_ms) * batch_ms
            if current_bucket is None:
                current_bucket = bucket
            if bucket != current_bucket:
                _flush_batch(current_bucket, batch)
                batch = []
                current_bucket = bucket
            batch.append(event)
        if current_bucket is not None and batch:
            _flush_batch(current_bucket, batch)

        stats = {
            "source_mode": "market_event_replay",
            "expected_events": len(events),
            "processed_events": 0,
            "frames_total": len(frames),
            "batch_ms": batch_ms,
            "window_start_ms": int(frames[0]["ts_ms"]) if frames else None,
            "window_end_ms": int(frames[-1]["ts_ms"]) if frames else None,
        }
        return frames, stats

    def _rolling_normalize(self, data: np.ndarray, window: int) -> np.ndarray:
        out = np.zeros_like(data, dtype=np.float32)
        for i in range(data.shape[0]):
            start = max(0, i - window + 1)
            segment = data[start : i + 1]
            mean = segment.mean(axis=0)
            std = segment.std(axis=0)
            std = np.where(std < 1e-6, 1.0, std)
            out[i] = (data[i] - mean) / std
        return out

    @staticmethod
    def _compact_feature_row(payload: dict[str, Any]) -> dict[str, Any]:
        # Keep only fields needed by dataset builder/labeling to reduce peak RAM.
        out: dict[str, Any] = {
            "ts_ms": int(payload.get("ts_ms", 0) or 0),
            "label_price": float(payload.get("label_price", 0.0) or 0.0),
            "is_stale_row": bool(payload.get("is_stale_row", False)),
        }
        for col in FEATURE_COLUMNS:
            out[col] = float(payload.get(col, 0.0) or 0.0)
        return out

    def _resolve_normalized_features_matrix(self, dataset: dict[str, Any]) -> np.ndarray:
        path_raw = str(dataset.get("normalized_features_path", "") or "").strip()
        if path_raw:
            path = Path(path_raw)
            if path.exists():
                return np.load(path, mmap_mode="r")
        return np.asarray(dataset.get("normalized_features", []), dtype=np.float32)

    @staticmethod
    def _target_class_names(target_mode: str) -> list[str]:
        mode = str(target_mode or "triple_barrier").strip().lower()
        if mode == "trade_outcome":
            return ["short_good", "no_trade", "long_good"]
        return ["down", "flat", "up"]

    @staticmethod
    def _is_torch_oom(exc: Exception) -> bool:
        message = str(exc).lower()
        return (
            "out of memory" in message
            or "cuda out of memory" in message
            or "ram guard" in message
        )

    @staticmethod
    def _as_bool(value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    async def _train_model(
        self,
        dataset: dict[str, Any],
        config: dict[str, Any],
        run_dir: Path,
        run_id: str,
        pair_symbol: str,
        instance_id: str,
    ) -> dict[str, Any] | None:
        try:
            import torch
            from torch import nn
            import torch.nn.functional as F
            from torch.utils.data import DataLoader, Dataset, WeightedRandomSampler
            from app.services.ml.model_arch import LSTMAttentionModel, SmallTCNModel, SmallTransformerEncoderModel
        except Exception as exc:
            await self._emit_run_log(run_id, "ERROR", f"PyTorch is required for training: {exc}")
            return None

        preferred_device = "cuda" if torch.cuda.is_available() else "cpu"
        await self._repository.update_run(run_id, {"device": preferred_device, "updated_at": utc_now_iso()})
        await self._emit_run_update(run_id)
        await self._emit_run_log(run_id, "INFO", f"Training device: {preferred_device}")
        if preferred_device == "cuda":
            try:
                free_mem, total_mem = torch.cuda.mem_get_info()
                await self._emit_run_log(
                    run_id,
                    "INFO",
                    f"CUDA memory available: {free_mem / (1024**3):.2f} GiB free / {total_mem / (1024**3):.2f} GiB total",
                )
            except Exception:
                pass

        normalized_features_np = self._resolve_normalized_features_matrix(dataset)
        lookback_steps = int(dataset["lookback_steps"])
        y_dir_train_np = np.asarray(dataset["y_dir_train"], dtype=np.int64)
        y_dir_val_np = np.asarray(dataset["y_dir_val"], dtype=np.int64)
        y_mag_train_np = np.asarray(dataset["y_mag_train"], dtype=np.float32)
        y_mag_val_np = np.asarray(dataset["y_mag_val"], dtype=np.float32)
        y_quality_train_np = np.asarray(dataset["y_quality_train"], dtype=np.float32)
        y_quality_val_np = np.asarray(dataset["y_quality_val"], dtype=np.float32)
        window_indices_train_np = np.asarray(dataset["window_indices_train"], dtype=np.int64)
        window_indices_val_np = np.asarray(dataset["window_indices_val"], dtype=np.int64)
        windows_total = int(dataset.get("samples_total", len(window_indices_train_np) + len(window_indices_val_np)))
        ram_budget_bytes = int(self._training_memory_budget_bytes(config=config))
        ram_budget_gb = float(ram_budget_bytes / float(1024**3))
        training_rss_cap_bytes = int(max(256 * 1024 * 1024, ram_budget_bytes * 0.98))
        prefetch_enabled = bool(config.get("training_prefetch_enabled", False))
        await self._merge_run_metrics(
            run_id,
            {
                "ram_budget_gb": float(round(ram_budget_gb, 6)),
                "windows_total": windows_total,
                "windows_processed": 0,
                "training_prefetch_enabled": prefetch_enabled,
            },
            emit_update=True,
        )
        if prefetch_enabled:
            await self._emit_run_log(
                run_id,
                "INFO",
                "Prefetch mode enabled (queue=1). Fallback to standard streaming will apply automatically on prefetch failures.",
            )
        else:
            await self._emit_run_log(run_id, "INFO", "Prefetch mode disabled. Using standard streaming path.")

        class _WindowIndexDataset(Dataset):
            def __init__(
                self,
                normalized: np.ndarray,
                window_indices: np.ndarray,
                y_dir: np.ndarray,
                y_mag: np.ndarray,
                y_quality: np.ndarray,
                lookback: int,
            ) -> None:
                self.normalized = normalized
                self.window_indices = window_indices
                self.y_dir = y_dir
                self.y_mag = y_mag
                self.y_quality = y_quality
                self.lookback = lookback

            def __len__(self) -> int:
                return int(len(self.window_indices))

            def __getitem__(self, idx: int) -> tuple[Any, Any, Any, Any]:
                row_idx = int(self.window_indices[idx])
                start = row_idx - self.lookback
                window = self.normalized[start:row_idx]
                x = torch.from_numpy(window.copy()).to(dtype=torch.float32)
                y_dir = torch.tensor(int(self.y_dir[idx]), dtype=torch.long)
                y_mag = torch.tensor(float(self.y_mag[idx]), dtype=torch.float32)
                y_quality = torch.tensor(float(self.y_quality[idx]), dtype=torch.float32)
                return x, y_dir, y_mag, y_quality

        base_batch_size = max(1, int(config["training"]["batch_size"]))
        epochs = int(config["training"]["epochs"])
        learning_rate = float(config["training"]["learning_rate"])
        hidden_size = int(config["training"]["hidden_size"])
        num_layers = int(config["training"]["num_layers"])
        dropout = float(config["training"]["dropout"])
        model_type = str(config["training"].get("model_type", "lstm_attention") or "lstm_attention").strip().lower()
        warm_start_from_current = self._as_bool(config["training"].get("warm_start_from_current", False), False)
        early_stopping_enabled = self._as_bool(config["training"].get("early_stopping_enabled", False), False)
        early_stopping_patience = max(1, int(config["training"].get("early_stopping_patience", 1)))
        early_stopping_min_delta = max(0.0, float(config["training"].get("early_stopping_min_delta", 0.0)))
        epoch_trace_capture_raw = config["training"].get("epoch_trace_capture_list", "")
        epoch_trace_capture_list = self._parse_epoch_trace_capture_list(epoch_trace_capture_raw, epochs)
        capture_trace_splits = self._enabled_trace_splits(config)
        await self._emit_run_log(run_id, "INFO", f"Warm start from current model: enabled={warm_start_from_current}")
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                "Early stopping config: "
                f"enabled={early_stopping_enabled}, "
                f"patience={early_stopping_patience}, "
                f"min_delta={early_stopping_min_delta:g}"
            ),
        )
        await self._emit_run_log(
            run_id,
            "INFO",
            f"Epoch trace capture list: [{', '.join(str(v) for v in epoch_trace_capture_list)}]",
        )
        await self._emit_run_log(
            run_id,
            "INFO",
            f"Trace capture splits: {', '.join(capture_trace_splits) if capture_trace_splits else 'none'}",
        )

        class_counts = np.bincount(y_dir_train_np, minlength=3)
        weighted_loss_enabled = self._as_bool(config["training"].get("use_class_weighted_loss", True), True)
        class_weight_mode = str(config["training"].get("class_weight_mode", "auto") or "auto").strip().lower()
        if class_weight_mode not in {"auto", "manual"}:
            class_weight_mode = "auto"
        use_focal_loss = self._as_bool(config["training"].get("use_focal_loss", False), False)
        focal_gamma = max(0.0, float(config["training"].get("focal_gamma", 2.0)))
        focal_use_alpha = self._as_bool(config["training"].get("focal_use_alpha_class_weights", True), True)
        use_balanced_sampler = self._as_bool(config["training"].get("use_balanced_sampler", False), False)
        use_class_quota_batches = self._as_bool(config["training"].get("use_class_quota_batches", False), False)
        class_quota_per_batch = max(0, int(config["training"].get("class_quota_per_batch", 4)))
        label_smoothing = max(0.0, min(0.2, float(config["training"].get("label_smoothing", 0.0))))
        early_stopping_monitor = str(config["training"].get("early_stopping_monitor", "val_loss") or "val_loss").strip().lower()
        if early_stopping_monitor not in {"val_loss", "macro_f1"}:
            early_stopping_monitor = "val_loss"

        class_weights = np.ones(3, dtype=np.float32)
        if weighted_loss_enabled:
            if class_weight_mode == "manual":
                class_weights = np.array(
                    [
                        max(0.0, float(config["training"].get("class_weight_down", 1.0))),
                        max(0.0, float(config["training"].get("class_weight_flat", 1.0))),
                        max(0.0, float(config["training"].get("class_weight_up", 1.0))),
                    ],
                    dtype=np.float32,
                )
                if float(class_weights.sum()) <= 0.0:
                    class_weights = np.ones(3, dtype=np.float32)
            else:
                class_weights = np.zeros(3, dtype=np.float32)
                total_rows = max(1, len(y_dir_train_np))
                for idx, count in enumerate(class_counts):
                    class_weights[idx] = float(total_rows) / float(max(1, count))
            class_weights = class_weights / max(1e-8, float(class_weights.sum())) * 3.0

        await self._emit_run_log(
            run_id,
            "INFO",
            (
                f"Direction class weights: enabled={weighted_loss_enabled}, mode={class_weight_mode}, "
                f"counts=[down:{int(class_counts[0])}, flat:{int(class_counts[1])}, up:{int(class_counts[2])}], "
                f"weights=[down:{float(class_weights[0]):.4f}, flat:{float(class_weights[1]):.4f}, up:{float(class_weights[2]):.4f}]"
            ),
        )
        await self._emit_run_log(
            run_id,
            "INFO",
            f"Direction loss mode: {'focal' if use_focal_loss else 'cross_entropy'} "
            f"(gamma={focal_gamma:.3f}, alpha_from_class_weights={focal_use_alpha})",
        )
        await self._emit_run_log(run_id, "INFO", f"Balanced sampler: enabled={use_balanced_sampler}")
        await self._emit_run_log(
            run_id,
            "INFO",
            (
                f"Batch quota: enabled={use_class_quota_batches}, per_class={class_quota_per_batch}; "
                f"label_smoothing={label_smoothing:.4f}; early_stop_monitor={early_stopping_monitor}"
            ),
        )

        train_ds = _WindowIndexDataset(
            normalized_features_np,
            window_indices_train_np,
            y_dir_train_np,
            y_mag_train_np,
            y_quality_train_np,
            lookback_steps,
        )
        val_ds = _WindowIndexDataset(
            normalized_features_np,
            window_indices_val_np,
            y_dir_val_np,
            y_mag_val_np,
            y_quality_val_np,
            lookback_steps,
        )

        batch_candidates: list[int] = []
        candidate = base_batch_size
        while candidate >= 1:
            if candidate not in batch_candidates:
                batch_candidates.append(candidate)
            if candidate == 1:
                break
            candidate = max(1, candidate // 2)
        if batch_candidates[-1] != 1:
            batch_candidates.append(1)

        device_attempts = [preferred_device]
        if preferred_device == "cuda":
            device_attempts.append("cpu")

        last_oom_error: Exception | None = None

        for attempt_device in device_attempts:
            effective_batch_candidates = batch_candidates if attempt_device == "cuda" else [batch_candidates[-1]]
            if attempt_device == "cpu" and preferred_device == "cuda":
                await self._emit_run_log(run_id, "WARNING", "Falling back to CPU training after CUDA memory pressure.")
            for attempt_batch in effective_batch_candidates:
                if attempt_device == "cuda":
                    await self._emit_run_log(
                        run_id,
                        "INFO",
                        f"CUDA training attempt with batch_size={attempt_batch}",
                    )
                try:
                    if attempt_device == "cuda":
                        try:
                            torch.cuda.empty_cache()
                        except Exception:
                            pass
                    gc.collect()

                    if model_type == "tcn":
                        model = SmallTCNModel(
                            input_size=int(normalized_features_np.shape[-1]),
                            hidden_channels=hidden_size,
                            dropout=dropout,
                        ).to(attempt_device)
                    elif model_type == "transformer_encoder":
                        model = SmallTransformerEncoderModel(
                            input_size=int(normalized_features_np.shape[-1]),
                            hidden_size=hidden_size,
                            num_layers=2,
                            num_heads=4,
                            ff_size=128,
                            dropout=dropout,
                            max_len=max(128, lookback_steps + 8),
                        ).to(attempt_device)
                    else:
                        model = LSTMAttentionModel(
                            input_size=int(normalized_features_np.shape[-1]),
                            hidden_size=hidden_size,
                            num_layers=num_layers,
                            dropout=dropout,
                        ).to(attempt_device)
                    if warm_start_from_current:
                        current_model = await self._repository.current_model(pair_symbol, instance_id)
                        if current_model and Path(str(current_model.get("model_path", ""))).exists():
                            try:
                                warm_bundle = torch.load(str(current_model["model_path"]), map_location="cpu")
                                warm_state = warm_bundle.get("state_dict")
                                if isinstance(warm_state, dict):
                                    model.load_state_dict(warm_state, strict=False)
                                    await self._emit_run_log(
                                        run_id,
                                        "INFO",
                                        f"Warm start loaded from run {str(current_model.get('run_id', 'unknown'))}.",
                                    )
                                else:
                                    await self._emit_run_log(
                                        run_id,
                                        "WARNING",
                                        "Warm start requested, but current model checkpoint has no state_dict. Starting fresh.",
                                    )
                            except Exception as exc:
                                await self._emit_run_log(
                                    run_id,
                                    "WARNING",
                                    f"Warm start skipped ({exc}). Starting from fresh initialization.",
                                )
                        else:
                            await self._emit_run_log(
                                run_id,
                                "WARNING",
                                "Warm start requested, but no approved current model found. Starting fresh.",
                            )

                    ce_weight = None
                    if weighted_loss_enabled:
                        ce_weight = torch.tensor(class_weights, dtype=torch.float32, device=attempt_device)

                    def direction_loss_fn(logits: Any, targets: Any) -> Any:
                        if not use_focal_loss:
                            return F.cross_entropy(
                                logits,
                                targets,
                                weight=ce_weight,
                                label_smoothing=label_smoothing,
                            )
                        ce_each = F.cross_entropy(
                            logits,
                            targets,
                            weight=ce_weight if focal_use_alpha else None,
                            label_smoothing=label_smoothing,
                            reduction="none",
                        )
                        pt = torch.exp(-ce_each)
                        focal = ((1.0 - pt).clamp(min=0.0) ** focal_gamma) * ce_each
                        return focal.mean()

                    mse_loss = nn.MSELoss()
                    bce_loss = nn.BCEWithLogitsLoss()
                    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)
                    scheduler = None
                    use_mixed_precision = attempt_device == "cuda"
                    scaler = torch.cuda.amp.GradScaler(enabled=use_mixed_precision)
                    # Local dataset class is not picklable for multiprocessing workers on Windows.
                    # Keep loaders single-process here to avoid "Can't pickle local object ..." failures.
                    loader_prefetch_enabled = False
                    loader_workers = 0
                    loader_prefetch_factor = 2
                    if bool(prefetch_enabled and attempt_device == "cuda"):
                        await self._emit_run_log(
                            run_id,
                            "WARNING",
                            "Prefetch requested but disabled for this training path to avoid Windows pickle worker failures.",
                        )
                    train_loader_kwargs: dict[str, Any] = {
                        "dataset": train_ds,
                        "batch_size": attempt_batch,
                        "shuffle": (not use_balanced_sampler and not use_class_quota_batches),
                        "pin_memory": (attempt_device == "cuda"),
                        "sampler": (
                            WeightedRandomSampler(
                                weights=torch.tensor(
                                    [
                                        float(len(y_dir_train_np)) / float(max(1, class_counts[int(cls_idx)]))
                                        for cls_idx in y_dir_train_np
                                    ],
                                    dtype=torch.float32,
                                ),
                                num_samples=len(y_dir_train_np),
                                replacement=True,
                            )
                            if use_balanced_sampler
                            else None
                        ),
                        "num_workers": loader_workers,
                    }
                    val_loader_kwargs: dict[str, Any] = {
                        "dataset": val_ds,
                        "batch_size": max(1, min(attempt_batch * 2, 1024)),
                        "shuffle": False,
                        "pin_memory": (attempt_device == "cuda"),
                        "num_workers": loader_workers,
                    }
                    if loader_workers > 0:
                        train_loader_kwargs["prefetch_factor"] = loader_prefetch_factor
                        train_loader_kwargs["persistent_workers"] = True
                        val_loader_kwargs["prefetch_factor"] = loader_prefetch_factor
                        val_loader_kwargs["persistent_workers"] = True
                    try:
                        train_loader = DataLoader(**train_loader_kwargs)
                        val_loader = DataLoader(**val_loader_kwargs)
                    except Exception as loader_exc:
                        if loader_prefetch_enabled:
                            await self._emit_run_log(
                                run_id,
                                "WARNING",
                                f"Prefetch loader init failed; auto-falling back to non-prefetch ({loader_exc})",
                            )
                            loader_prefetch_enabled = False
                            train_loader_kwargs["num_workers"] = 0
                            val_loader_kwargs["num_workers"] = 0
                            train_loader_kwargs.pop("prefetch_factor", None)
                            train_loader_kwargs.pop("persistent_workers", None)
                            val_loader_kwargs.pop("prefetch_factor", None)
                            val_loader_kwargs.pop("persistent_workers", None)
                            train_loader = DataLoader(**train_loader_kwargs)
                            val_loader = DataLoader(**val_loader_kwargs)
                            await self._merge_run_metrics(
                                run_id,
                                {"training_prefetch_enabled": False},
                                emit_update=True,
                            )
                        else:
                            raise
                    if loader_prefetch_enabled != bool(prefetch_enabled):
                        await self._merge_run_metrics(
                            run_id,
                            {"training_prefetch_enabled": bool(loader_prefetch_enabled)},
                            emit_update=True,
                        )

                    checkpoint_payload = await self._load_stream_checkpoint(
                        run_dir=run_dir,
                        run_id=run_id,
                        torch_module=torch,
                    )
                    resumed_from_checkpoint = False
                    start_epoch = 0
                    chunk_cursor = 0
                    if isinstance(checkpoint_payload, dict):
                        try:
                            model_state = checkpoint_payload.get("model_state_dict")
                            if isinstance(model_state, dict):
                                model.load_state_dict(model_state, strict=False)
                            optimizer_state = checkpoint_payload.get("optimizer_state_dict")
                            if isinstance(optimizer_state, dict):
                                optimizer.load_state_dict(optimizer_state)
                            scaler_state = checkpoint_payload.get("scaler_state_dict")
                            if isinstance(scaler_state, dict):
                                scaler.load_state_dict(scaler_state)
                            scheduler_state = checkpoint_payload.get("scheduler_state_dict")
                            if scheduler is not None and isinstance(scheduler_state, dict):
                                scheduler.load_state_dict(scheduler_state)
                            rng_state = checkpoint_payload.get("rng_state")
                            if isinstance(rng_state, dict):
                                self._restore_rng_state(torch, rng_state)
                            start_epoch = max(0, int(checkpoint_payload.get("epoch_index", -1)) + 1)
                            chunk_cursor = max(0, int(checkpoint_payload.get("chunk_index", 0)))
                            if start_epoch >= epochs:
                                start_epoch = max(0, epochs - 1)
                            resumed_from_checkpoint = True
                            await self._emit_run_log(
                                run_id,
                                "INFO",
                                f"Resuming training from checkpoint epoch={start_epoch} chunk={chunk_cursor}.",
                            )
                        except Exception as exc:
                            await self._emit_run_log(run_id, "WARNING", f"Checkpoint restore failed; training from scratch ({exc})")
                            start_epoch = 0
                            chunk_cursor = 0
                            resumed_from_checkpoint = False
                    await self._merge_run_metrics(
                        run_id,
                        {
                            "resumed_from_checkpoint": bool(resumed_from_checkpoint),
                            "checkpoint_step": f"epoch={start_epoch},chunk={chunk_cursor}" if resumed_from_checkpoint else "",
                        },
                        emit_update=True,
                    )

                    best_metric = float("-inf") if early_stopping_monitor == "macro_f1" else float("inf")
                    best_val_loss = float("inf")
                    best_state: dict[str, Any] | None = None
                    no_improve_epochs = 0
                    class_indices: dict[int, np.ndarray] = {
                        0: np.where(y_dir_train_np == 0)[0],
                        1: np.where(y_dir_train_np == 1)[0],
                        2: np.where(y_dir_train_np == 2)[0],
                    }
                    all_train_indices = np.arange(len(y_dir_train_np), dtype=np.int64)
                    windows_processed = 0

                    for epoch in range(start_epoch, epochs):
                        control = self._controls.setdefault(run_id, _RunControl())
                        if control.stop:
                            return None
                        while control.pause and not control.stop:
                            await asyncio.sleep(0.5)

                        model.train()
                        epoch_loss = 0.0
                        train_batches = 0
                        if use_class_quota_batches:
                            effective_quota = min(max(0, class_quota_per_batch), max(0, attempt_batch // 3))
                            num_steps = max(1, math.ceil(len(y_dir_train_np) / max(1, attempt_batch)))
                            batch_index_groups: list[np.ndarray] = []
                            for _ in range(num_steps):
                                picked: list[int] = []
                                if effective_quota > 0:
                                    for cls_id in (0, 1, 2):
                                        cls_pool = class_indices.get(cls_id)
                                        if cls_pool is None or len(cls_pool) == 0:
                                            continue
                                        cls_pick = np.random.choice(cls_pool, size=effective_quota, replace=True)
                                        picked.extend(int(v) for v in cls_pick.tolist())
                                remaining = max(0, attempt_batch - len(picked))
                                if remaining > 0:
                                    extra = np.random.choice(all_train_indices, size=remaining, replace=True)
                                    picked.extend(int(v) for v in extra.tolist())
                                if not picked:
                                    continue
                                picked_arr = np.asarray(picked, dtype=np.int64)
                                np.random.shuffle(picked_arr)
                                batch_index_groups.append(picked_arr)
                            iterable_batches = batch_index_groups
                        else:
                            iterable_batches = train_loader
                        try:
                            total_batches_est = max(1, int(len(iterable_batches)))  # type: ignore[arg-type]
                        except Exception:
                            total_batches_est = max(1, int(math.ceil(len(y_dir_train_np) / max(1, attempt_batch))))

                        for batch_idx, batch in enumerate(iterable_batches, start=1):
                            if use_class_quota_batches:
                                idx = torch.from_numpy(batch)
                                picked_rows = window_indices_train_np[idx.cpu().numpy()]
                                windows_np = np.stack(
                                    [
                                        normalized_features_np[int(row_idx) - lookback_steps : int(row_idx)]
                                        for row_idx in picked_rows
                                    ],
                                    axis=0,
                                ).astype(np.float32, copy=False)
                                x_b = torch.from_numpy(windows_np).to(attempt_device, non_blocking=True)
                                y_dir_b = torch.from_numpy(y_dir_train_np[idx.cpu().numpy()]).to(attempt_device, non_blocking=True)
                                y_mag_b = torch.from_numpy(y_mag_train_np[idx.cpu().numpy()]).to(attempt_device, non_blocking=True)
                                y_quality_b = torch.from_numpy(y_quality_train_np[idx.cpu().numpy()]).to(attempt_device, non_blocking=True)
                            else:
                                x_b, y_dir_b, y_mag_b, y_quality_b = [item.to(attempt_device, non_blocking=True) for item in batch]
                            optimizer.zero_grad(set_to_none=True)
                            if use_mixed_precision:
                                with torch.autocast(device_type="cuda", dtype=torch.float16):
                                    dir_logits, mag_out, quality_logits = model(x_b)
                                    loss_dir = direction_loss_fn(dir_logits, y_dir_b)
                                    loss_mag = mse_loss(mag_out, y_mag_b)
                                    loss_quality = bce_loss(quality_logits, y_quality_b)
                                    loss = 0.5 * loss_dir + 0.3 * loss_mag + 0.2 * loss_quality
                                scaler.scale(loss).backward()
                                scaler.step(optimizer)
                                scaler.update()
                            else:
                                dir_logits, mag_out, quality_logits = model(x_b)
                                loss_dir = direction_loss_fn(dir_logits, y_dir_b)
                                loss_mag = mse_loss(mag_out, y_mag_b)
                                loss_quality = bce_loss(quality_logits, y_quality_b)
                                loss = 0.5 * loss_dir + 0.3 * loss_mag + 0.2 * loss_quality
                                loss.backward()
                                optimizer.step()
                            epoch_loss += float(loss.item())
                            train_batches += 1
                            windows_processed += int(getattr(x_b, "shape", [0])[0] or 0)
                            chunk_cursor += 1
                            if batch_idx == 1 or (batch_idx % 50 == 0):
                                await self._hub.broadcast(
                                    {
                                        "type": "ml_training_batch",
                                        "run_id": run_id,
                                        "pair_symbol": pair_symbol,
                                        "instance_id": str(instance_id),
                                        "payload": {
                                            "epoch": int(epoch + 1),
                                            "epochs_total": int(epochs),
                                            "batch": int(batch_idx),
                                            "batches_total": int(total_batches_est),
                                            "train_loss": float(loss.item()),
                                            "device": str(attempt_device),
                                            "batch_size": int(attempt_batch),
                                        },
                                    }
                                )
                            if chunk_cursor % 50 == 0:
                                await self._save_stream_checkpoint(
                                    run_dir=run_dir,
                                    run_id=run_id,
                                    torch_module=torch,
                                    model=model,
                                    optimizer=optimizer,
                                    scaler=scaler,
                                    scheduler=scheduler,
                                    epoch_index=int(epoch),
                                    chunk_index=int(chunk_cursor),
                                    split_cursor={"train": int(min(len(y_dir_train_np), windows_processed)), "val": 0, "test": 0},
                                    progress={
                                        "windows_processed": int(min(windows_total, windows_processed)),
                                        "windows_total": int(windows_total),
                                        "train_steps_processed": int(chunk_cursor),
                                    },
                                )
                                await self._merge_run_metrics(
                                    run_id,
                                    {
                                        "train_steps_processed": int(chunk_cursor),
                                        "windows_processed": int(min(windows_total, windows_processed)),
                                        "checkpoint_step": f"epoch={epoch + 1},train_step={chunk_cursor}",
                                    },
                                    emit_update=True,
                                )
                            if (batch_idx % 25) == 0:
                                rss_now = self._process_tree_resident_memory_bytes() or 0
                                if rss_now >= training_rss_cap_bytes:
                                    raise RuntimeError(
                                        f"Training RAM guard exceeded: rss_gb={rss_now / float(1024**3):.3f} "
                                        f"limit_gb={training_rss_cap_bytes / float(1024**3):.3f}"
                                    )

                        model.eval()
                        val_loss_weighted_sum = 0.0
                        val_samples = 0
                        val_true: list[int] = []
                        val_pred: list[int] = []
                        with torch.no_grad():
                            for v_batch in val_loader:
                                x_v, y_dir_v, y_mag_v, y_quality_v = [
                                    item.to(attempt_device, non_blocking=True) for item in v_batch
                                ]
                                if use_mixed_precision:
                                    with torch.autocast(device_type="cuda", dtype=torch.float16):
                                        dir_logits_val, mag_val, quality_val = model(x_v)
                                        v_loss_dir = direction_loss_fn(dir_logits_val, y_dir_v)
                                        v_loss_mag = mse_loss(mag_val, y_mag_v)
                                        v_loss_quality = bce_loss(quality_val, y_quality_v)
                                        v_loss = 0.5 * v_loss_dir + 0.3 * v_loss_mag + 0.2 * v_loss_quality
                                else:
                                    dir_logits_val, mag_val, quality_val = model(x_v)
                                    v_loss_dir = direction_loss_fn(dir_logits_val, y_dir_v)
                                    v_loss_mag = mse_loss(mag_val, y_mag_v)
                                    v_loss_quality = bce_loss(quality_val, y_quality_v)
                                    v_loss = 0.5 * v_loss_dir + 0.3 * v_loss_mag + 0.2 * v_loss_quality
                                bsz = int(x_v.shape[0])
                                pred_cls = torch.argmax(torch.softmax(dir_logits_val, dim=1), dim=1)
                                val_true.extend(y_dir_v.detach().cpu().tolist())
                                val_pred.extend(pred_cls.detach().cpu().tolist())
                                val_loss_weighted_sum += float(v_loss.item()) * bsz
                                val_samples += bsz
                        val_loss = val_loss_weighted_sum / max(1, val_samples)
                        cm = [[0, 0, 0], [0, 0, 0], [0, 0, 0]]
                        for t_cls, p_cls in zip(val_true, val_pred):
                            if 0 <= int(t_cls) < 3 and 0 <= int(p_cls) < 3:
                                cm[int(t_cls)][int(p_cls)] += 1
                        val_dir_acc = float(sum(cm[i][i] for i in (0, 1, 2)) / max(1, len(val_true)))
                        precision_by_class: dict[str, float] = {}
                        recall_by_class: dict[str, float] = {}
                        f1_by_class: dict[str, float] = {}
                        macro_f1 = 0.0
                        for idx, cls_name in ((0, "down"), (1, "flat"), (2, "up")):
                            tp = float(cm[idx][idx])
                            fp = float(sum(cm[r][idx] for r in (0, 1, 2) if r != idx))
                            fn = float(sum(cm[idx][c] for c in (0, 1, 2) if c != idx))
                            precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
                            recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
                            f1 = (2.0 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
                            precision_by_class[cls_name] = float(precision)
                            recall_by_class[cls_name] = float(recall)
                            f1_by_class[cls_name] = float(f1)
                            macro_f1 += f1
                        macro_f1 /= 3.0
                        epoch_metrics = {
                            "epoch": int(epoch + 1),
                            "epochs_total": int(epochs),
                            "train_loss": float(epoch_loss / max(1, train_batches)),
                            "val_loss": float(val_loss),
                            "val_macro_f1": float(macro_f1),
                            "direction_accuracy": float(val_dir_acc),
                            "direction_precision_down": float(precision_by_class.get("down", 0.0)),
                            "direction_precision_flat": float(precision_by_class.get("flat", 0.0)),
                            "direction_precision_up": float(precision_by_class.get("up", 0.0)),
                            "direction_recall_down": float(recall_by_class.get("down", 0.0)),
                            "direction_recall_flat": float(recall_by_class.get("flat", 0.0)),
                            "direction_recall_up": float(recall_by_class.get("up", 0.0)),
                            "direction_f1_down": float(f1_by_class.get("down", 0.0)),
                            "direction_f1_flat": float(f1_by_class.get("flat", 0.0)),
                            "direction_f1_up": float(f1_by_class.get("up", 0.0)),
                            "direction_confusion_matrix": cm,
                            "batch_size": int(attempt_batch),
                            "device": str(attempt_device),
                        }
                        await self._repository.append_run_epoch_metrics(run_id, int(epoch + 1), epoch_metrics)
                        if int(epoch + 1) in epoch_trace_capture_list:
                            for trace_split in capture_trace_splits:
                                await self._capture_epoch_prediction_trace(
                                    run_id=run_id,
                                    pair_symbol=pair_symbol,
                                    epoch_index=int(epoch + 1),
                                    split=trace_split,
                                    model=model,
                                    dataset=dataset,
                                    config=config,
                                )
                        await self._hub.broadcast(
                            {
                                "type": "ml_training_epoch",
                                "run_id": run_id,
                                "pair_symbol": pair_symbol,
                                "instance_id": str(instance_id),
                                "payload": epoch_metrics,
                            }
                        )

                        epoch_progress = 0.45 + ((epoch + 1) / max(1, epochs)) * 0.40
                        await self._repository.update_run(
                            run_id,
                            {
                                "stage": "training",
                                "stage_progress": epoch_progress,
                                "updated_at": utc_now_iso(),
                            },
                        )
                        await self._emit_run_log(
                            run_id,
                            "INFO",
                            (
                                f"Epoch {epoch + 1}/{epochs} train_loss={epoch_loss / max(1, train_batches):.5f} "
                                f"val_loss={val_loss:.5f} val_macro_f1={macro_f1:.5f} batch={attempt_batch} device={attempt_device}"
                            ),
                        )
                        await self._save_stream_checkpoint(
                            run_dir=run_dir,
                            run_id=run_id,
                            torch_module=torch,
                            model=model,
                            optimizer=optimizer,
                            scaler=scaler,
                            scheduler=scheduler,
                            epoch_index=int(epoch),
                            chunk_index=int(chunk_cursor),
                            split_cursor={"train": int(min(len(y_dir_train_np), windows_processed)), "val": int(len(y_dir_val_np)), "test": 0},
                            progress={
                                "windows_processed": int(min(windows_total, windows_processed)),
                                "windows_total": int(windows_total),
                                "train_steps_processed": int(chunk_cursor),
                            },
                        )
                        await self._merge_run_metrics(
                            run_id,
                            {
                                "train_steps_processed": int(chunk_cursor),
                                "windows_processed": int(min(windows_total, windows_processed)),
                                "checkpoint_step": f"epoch={epoch + 1},train_step={chunk_cursor}",
                            },
                            emit_update=True,
                        )
                        await self._runtime_boundary_cleanup(
                            run_id=run_id,
                            reason="epoch_end",
                            torch_module=torch,
                        )
                        await self._emit_run_update(run_id)
                        best_val_loss = min(best_val_loss, val_loss)
                        if early_stopping_monitor == "macro_f1":
                            current_metric = macro_f1
                            improved = current_metric > (best_metric + early_stopping_min_delta)
                        else:
                            current_metric = val_loss
                            improved = current_metric < (best_metric - early_stopping_min_delta)
                        if improved:
                            best_metric = current_metric
                            best_state = {key: value.detach().cpu() for key, value in model.state_dict().items()}
                            no_improve_epochs = 0
                        else:
                            no_improve_epochs += 1
                            if early_stopping_enabled and no_improve_epochs >= early_stopping_patience:
                                await self._emit_run_log(
                                    run_id,
                                    "INFO",
                                    (
                                        "Early stopping triggered at "
                                        f"epoch {epoch + 1}/{epochs} (monitor={early_stopping_monitor}, patience={early_stopping_patience}, "
                                        f"min_delta={early_stopping_min_delta:g})."
                                    ),
                                )
                                break

                    if best_state is None:
                        return None

                    model_path = run_dir / "model.pt"
                    torch.save(
                        {
                            "state_dict": best_state,
                            "feature_names": dataset["feature_names"],
                            "lookback_steps": dataset["lookback_steps"],
                            "horizon": dataset["horizon"],
                            "model": {
                                "model_type": model_type,
                                "hidden_size": hidden_size,
                                "num_layers": num_layers,
                                "dropout": dropout,
                            },
                        },
                        model_path,
                    )
                    normalizer_path = run_dir / "normalizer_stats.json"
                    normalizer_path.write_text(
                        json.dumps(
                            {
                                "method": "rolling_zscore",
                                "window": int(config["normalizer_window"]),
                                "feature_names": dataset["feature_names"],
                            },
                            indent=2,
                        ),
                        encoding="utf-8",
                    )
                    if attempt_device != preferred_device:
                        await self._repository.update_run(run_id, {"device": attempt_device, "updated_at": utc_now_iso()})
                        await self._emit_run_update(run_id)
                    await self._save_stream_checkpoint(
                        run_dir=run_dir,
                        run_id=run_id,
                        torch_module=torch,
                        model=model,
                        optimizer=optimizer,
                        scaler=scaler,
                        scheduler=scheduler,
                        epoch_index=int(epochs),
                        chunk_index=int(chunk_cursor),
                        split_cursor={
                            "train": int(len(y_dir_train_np)),
                            "val": int(len(y_dir_val_np)),
                            "test": 0,
                        },
                        progress={
                            "windows_processed": int(windows_total),
                            "windows_total": int(windows_total),
                            "train_steps_processed": int(chunk_cursor),
                        },
                    )
                    await self._merge_run_metrics(
                        run_id,
                        {
                            "windows_processed": int(windows_total),
                            "windows_total": int(windows_total),
                            "train_steps_processed": int(chunk_cursor),
                            "loaded_now_gb": 0.0,
                            "checkpoint_step": f"epoch={epochs},train_step={chunk_cursor}",
                        },
                        emit_update=True,
                    )
                    return {
                        "device": attempt_device,
                        "model_path": str(model_path),
                        "normalizer_path": str(normalizer_path),
                        "best_val_loss": best_val_loss,
                    }
                except RuntimeError as exc:
                    if not self._is_torch_oom(exc):
                        raise
                    last_oom_error = exc
                    await self._emit_run_log(
                        run_id,
                        "WARNING",
                        (
                            f"OOM on device={attempt_device}, batch={attempt_batch}. "
                            "Trying a smaller memory profile."
                        ),
                    )
                    await self._runtime_boundary_cleanup(
                        run_id=run_id,
                        reason="stepdown_event",
                        torch_module=torch,
                        force=True,
                    )
                    try:
                        if attempt_device == "cuda":
                            torch.cuda.empty_cache()
                    except Exception:
                        pass
                    gc.collect()
                    continue

        if last_oom_error is not None:
            raise RuntimeError(
                "Training failed after adaptive memory retries. "
                f"Last error: {last_oom_error}"
            )
        return None

    @staticmethod
    def _normalize_training_ram_budget_gb(value: Any) -> float | None:
        if value is None:
            return None
        try:
            numeric = float(value)
        except Exception:
            return None
        if not math.isfinite(numeric):
            return None
        return float(max(0.5, min(64.0, numeric)))

    def _training_memory_budget_bytes(self, *, config: dict[str, Any] | None = None) -> int:
        mb = 1024 * 1024
        requested_gb: float | None = None
        if isinstance(config, dict):
            requested_gb = self._normalize_training_ram_budget_gb(config.get("training_ram_budget_gb"))
        if requested_gb is None:
            requested_gb = self._normalize_training_ram_budget_gb(self._workload_controls().get("training_ram_budget_gb"))
        if requested_gb is not None:
            return max(int(0.5 * 1024 * mb), int(requested_gb * 1024 * mb))
        available = self._available_system_memory_bytes()
        if available is None or available <= 0:
            return 512 * mb
        # Auto budget for bounded full-data training.
        budget = int(available * 0.30)
        return max(int(0.5 * 1024 * mb), min(int(12.0 * 1024 * mb), budget))

    def _available_system_memory_bytes(self) -> int | None:
        try:
            import psutil  # type: ignore

            return int(psutil.virtual_memory().available)
        except Exception:
            pass

        if os.name == "nt":
            try:
                class _MEMORYSTATUSEX(ctypes.Structure):
                    _fields_ = [
                        ("dwLength", ctypes.c_ulong),
                        ("dwMemoryLoad", ctypes.c_ulong),
                        ("ullTotalPhys", ctypes.c_ulonglong),
                        ("ullAvailPhys", ctypes.c_ulonglong),
                        ("ullTotalPageFile", ctypes.c_ulonglong),
                        ("ullAvailPageFile", ctypes.c_ulonglong),
                        ("ullTotalVirtual", ctypes.c_ulonglong),
                        ("ullAvailVirtual", ctypes.c_ulonglong),
                        ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                    ]

                status = _MEMORYSTATUSEX()
                status.dwLength = ctypes.sizeof(_MEMORYSTATUSEX)
                if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):  # type: ignore[attr-defined]
                    return int(status.ullAvailPhys)
            except Exception:
                return None
        return None

    def _process_resident_memory_bytes(self) -> int | None:
        try:
            import psutil  # type: ignore

            return int(psutil.Process(os.getpid()).memory_info().rss)
        except Exception:
            pass
        if os.name == "nt":
            try:
                from ctypes import wintypes

                class PROCESS_MEMORY_COUNTERS_EX(ctypes.Structure):
                    _fields_ = [
                        ("cb", wintypes.DWORD),
                        ("PageFaultCount", wintypes.DWORD),
                        ("PeakWorkingSetSize", ctypes.c_size_t),
                        ("WorkingSetSize", ctypes.c_size_t),
                        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                        ("PagefileUsage", ctypes.c_size_t),
                        ("PeakPagefileUsage", ctypes.c_size_t),
                        ("PrivateUsage", ctypes.c_size_t),
                    ]

                psapi = ctypes.WinDLL("Psapi.dll")
                kernel32 = ctypes.WinDLL("Kernel32.dll")
                get_proc_mem = psapi.GetProcessMemoryInfo
                get_proc_mem.argtypes = [
                    wintypes.HANDLE,
                    ctypes.POINTER(PROCESS_MEMORY_COUNTERS_EX),
                    wintypes.DWORD,
                ]
                get_proc_mem.restype = wintypes.BOOL
                get_current_process = kernel32.GetCurrentProcess
                get_current_process.argtypes = []
                get_current_process.restype = wintypes.HANDLE

                counters = PROCESS_MEMORY_COUNTERS_EX()
                counters.cb = wintypes.DWORD(ctypes.sizeof(PROCESS_MEMORY_COUNTERS_EX))
                ok = get_proc_mem(get_current_process(), ctypes.byref(counters), counters.cb)
                if ok:
                    return int(counters.WorkingSetSize)
            except Exception:
                pass
        return None

    def _process_tree_resident_memory_bytes(self) -> int | None:
        """Resident memory for current process + all descendants."""
        try:
            import psutil  # type: ignore

            root = psutil.Process(os.getpid())
            total = int(root.memory_info().rss)
            for child in root.children(recursive=True):
                try:
                    total += int(child.memory_info().rss)
                except Exception:
                    continue
            return total
        except Exception:
            pass
        return self._process_resident_memory_bytes()

    @staticmethod
    def _attach_windows_job_memory_cap(pid: int, limit_bytes: int) -> Any | None:
        """Attach a child pid to a Windows Job Object with process memory cap."""
        if os.name != "nt":
            return None
        try:
            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            PROCESS_SET_QUOTA = 0x0100
            PROCESS_TERMINATE = 0x0001
            PROCESS_QUERY_INFORMATION = 0x0400
            access = PROCESS_SET_QUOTA | PROCESS_TERMINATE | PROCESS_QUERY_INFORMATION
            h_process = kernel32.OpenProcess(access, False, int(pid))
            if not h_process:
                return None

            h_job = kernel32.CreateJobObjectW(None, None)
            if not h_job:
                kernel32.CloseHandle(h_process)
                return None

            class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
                _fields_ = [
                    ("PerProcessUserTimeLimit", ctypes.c_longlong),
                    ("PerJobUserTimeLimit", ctypes.c_longlong),
                    ("LimitFlags", ctypes.c_uint32),
                    ("MinimumWorkingSetSize", ctypes.c_size_t),
                    ("MaximumWorkingSetSize", ctypes.c_size_t),
                    ("ActiveProcessLimit", ctypes.c_uint32),
                    ("Affinity", ctypes.c_size_t),
                    ("PriorityClass", ctypes.c_uint32),
                    ("SchedulingClass", ctypes.c_uint32),
                ]

            class IO_COUNTERS(ctypes.Structure):
                _fields_ = [
                    ("ReadOperationCount", ctypes.c_ulonglong),
                    ("WriteOperationCount", ctypes.c_ulonglong),
                    ("OtherOperationCount", ctypes.c_ulonglong),
                    ("ReadTransferCount", ctypes.c_ulonglong),
                    ("WriteTransferCount", ctypes.c_ulonglong),
                    ("OtherTransferCount", ctypes.c_ulonglong),
                ]

            class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
                _fields_ = [
                    ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
                    ("IoInfo", IO_COUNTERS),
                    ("ProcessMemoryLimit", ctypes.c_size_t),
                    ("JobMemoryLimit", ctypes.c_size_t),
                    ("PeakProcessMemoryUsed", ctypes.c_size_t),
                    ("PeakJobMemoryUsed", ctypes.c_size_t),
                ]

            JOB_OBJECT_LIMIT_PROCESS_MEMORY = 0x00000100
            JobObjectExtendedLimitInformation = 9
            info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_PROCESS_MEMORY
            info.ProcessMemoryLimit = ctypes.c_size_t(max(256 * 1024 * 1024, int(limit_bytes)))
            ok = kernel32.SetInformationJobObject(
                h_job,
                JobObjectExtendedLimitInformation,
                ctypes.byref(info),
                ctypes.sizeof(info),
            )
            if not ok:
                kernel32.CloseHandle(h_job)
                kernel32.CloseHandle(h_process)
                return None

            ok = kernel32.AssignProcessToJobObject(h_job, h_process)
            kernel32.CloseHandle(h_process)
            if not ok:
                kernel32.CloseHandle(h_job)
                return None
            return h_job
        except Exception:
            return None

    async def _evaluate_model(
        self,
        dataset: dict[str, Any],
        train_result: dict[str, Any],
        config: dict[str, Any],
        run_id: str,
        pair_symbol: str,
    ) -> dict[str, Any]:
        try:
            import torch
            from app.services.ml.model_arch import LSTMAttentionModel, SmallTCNModel, SmallTransformerEncoderModel
        except Exception:
            return {"error": "PyTorch not available for evaluation"}
        bundle = torch.load(train_result["model_path"], map_location="cpu")
        model_cfg = bundle["model"]
        model_type = str(model_cfg.get("model_type", "lstm_attention") or "lstm_attention").strip().lower()
        if model_type == "tcn":
            model = SmallTCNModel(
                input_size=len(dataset["feature_names"]),
                hidden_channels=int(model_cfg["hidden_size"]),
                dropout=float(model_cfg["dropout"]),
            )
        elif model_type == "transformer_encoder":
            model = SmallTransformerEncoderModel(
                input_size=len(dataset["feature_names"]),
                hidden_size=int(model_cfg["hidden_size"]),
                num_layers=2,
                num_heads=4,
                ff_size=128,
                dropout=float(model_cfg["dropout"]),
                max_len=max(128, int(bundle.get("lookback_steps", dataset.get("lookback_steps", 60))) + 8),
            )
        else:
            model = LSTMAttentionModel(
                input_size=len(dataset["feature_names"]),
                hidden_size=int(model_cfg["hidden_size"]),
                num_layers=int(model_cfg["num_layers"]),
                dropout=float(model_cfg["dropout"]),
            )
        model.load_state_dict(bundle["state_dict"])
        model.eval()
        normalized_features_np = self._resolve_normalized_features_matrix(dataset)
        lookback_steps = int(dataset["lookback_steps"])
        window_indices_test_np = np.asarray(dataset["window_indices_test"], dtype=np.int64)
        y_dir_np = np.asarray(dataset["y_dir_test"], dtype=np.int64)
        y_mag_np = np.asarray(dataset["y_mag_test"], dtype=np.float32)
        y_quality_np = np.asarray(dataset["y_quality_test"], dtype=np.float32)
        y_ts_np = np.asarray(dataset.get("ts_test", np.arange(len(y_dir_np), dtype=np.int64)), dtype=np.int64)
        n_test = int(len(window_indices_test_np))
        if n_test <= 0:
            return {
                "direction_accuracy": 0.0,
                "direction_precision_macro": 0.0,
                "direction_recall_macro": 0.0,
                "direction_f1_macro": 0.0,
                "direction_precision_down": 0.0,
                "direction_precision_flat": 0.0,
                "direction_precision_up": 0.0,
                "direction_recall_down": 0.0,
                "direction_recall_flat": 0.0,
                "direction_recall_up": 0.0,
                "direction_f1_down": 0.0,
                "direction_f1_flat": 0.0,
                "direction_f1_up": 0.0,
                "direction_confusion_matrix": [[0, 0, 0], [0, 0, 0], [0, 0, 0]],
                "direction_class_order": ["down", "flat", "up"],
                "magnitude_rmse": 0.0,
                "quality_accuracy": 0.0,
                "pnl_proxy": 0.0,
                "sharpe_proxy": 0.0,
                "best_val_loss": round(float(train_result["best_val_loss"]), 6),
                "samples_test": 0,
            }

        eval_batch_size = max(128, min(int(config["training"].get("batch_size", 64)) * 8, 4096))
        temperature_scaling_enabled = self._as_bool(config["paper_bot"].get("temperature_scaling_enabled", True), True)
        temperature_value = 1.0
        if temperature_scaling_enabled:
            temperature_value = self._fit_temperature_from_validation(
                model=model,
                dataset=dataset,
                lookback_steps=lookback_steps,
                eval_batch_size=eval_batch_size,
            )
        confidence_threshold = float(config["paper_bot"]["confidence_threshold"])
        dir_correct = 0
        quality_correct = 0
        sq_err_sum = 0.0
        pnl_sum = 0.0
        pnl_count = 0
        y_pred_np = np.empty(n_test, dtype=np.int64)
        confidence_np = np.zeros(n_test, dtype=np.float32)
        confidence_gap_np = np.zeros(n_test, dtype=np.float32)
        quality_pred_np = np.zeros(n_test, dtype=np.float32)
        magnitude_pred_np = np.zeros(n_test, dtype=np.float32)
        prob_down_np = np.zeros(n_test, dtype=np.float32)
        prob_flat_np = np.zeros(n_test, dtype=np.float32)
        prob_up_np = np.zeros(n_test, dtype=np.float32)

        with torch.no_grad():
            for start in range(0, n_test, eval_batch_size):
                end = min(n_test, start + eval_batch_size)
                batch_rows = window_indices_test_np[start:end]
                windows_np = np.stack(
                    [
                        normalized_features_np[int(row_idx) - lookback_steps : int(row_idx)]
                        for row_idx in batch_rows
                    ],
                    axis=0,
                ).astype(np.float32, copy=False)
                x_b = torch.from_numpy(windows_np).to(dtype=torch.float32)
                y_dir_b = y_dir_np[start:end]
                y_mag_b = y_mag_np[start:end]
                y_quality_b = y_quality_np[start:end]

                dir_logits, mag_out, quality_logits = model(x_b)
                probs = torch.softmax(dir_logits / max(1e-6, float(temperature_value)), dim=1)
                pred_cls = torch.argmax(probs, dim=1).cpu().numpy().astype(np.int64)
                conf = torch.max(probs, dim=1).values.cpu().numpy().astype(np.float32)
                sorted_probs = torch.sort(probs, dim=1, descending=True).values.cpu().numpy().astype(np.float32)
                mag_pred = mag_out.cpu().numpy().astype(np.float32)
                quality_prob = torch.sigmoid(quality_logits).cpu().numpy().astype(np.float32)

                y_pred_np[start:end] = pred_cls
                confidence_np[start:end] = conf
                confidence_gap_np[start:end] = (sorted_probs[:, 0] - sorted_probs[:, 1]).astype(np.float32)
                quality_pred_np[start:end] = quality_prob
                magnitude_pred_np[start:end] = mag_pred
                prob_down_np[start:end] = probs[:, 0].cpu().numpy().astype(np.float32)
                prob_flat_np[start:end] = probs[:, 1].cpu().numpy().astype(np.float32)
                prob_up_np[start:end] = probs[:, 2].cpu().numpy().astype(np.float32)
                dir_correct += int(np.sum(pred_cls == y_dir_b))
                sq_err_sum += float(np.sum((mag_pred - y_mag_b) ** 2))
                quality_correct += int(np.sum((quality_prob > 0.5).astype(np.float32) == y_quality_b))

                valid_mask = conf > confidence_threshold
                if np.any(valid_mask):
                    signed_pred = np.where(pred_cls == 2, 1.0, np.where(pred_cls == 0, -1.0, 0.0)).astype(np.float32)
                    pnl_sum += float(np.sum((signed_pred * y_mag_b)[valid_mask]))
                    pnl_count += int(np.sum(valid_mask))

        direction_acc = float(dir_correct / max(1, n_test))
        magnitude_rmse = float(np.sqrt(sq_err_sum / max(1, n_test)))
        magnitude_rmse_pct = float(magnitude_rmse * 100.0)
        quality_acc = float(quality_correct / max(1, n_test))
        pnl_proxy = float(pnl_sum / max(1, pnl_count))

        # Direction-class metrics: class order is [DOWN(0), FLAT(1), UP(2)]
        y_true_np = y_dir_np
        class_ids = [0, 1, 2]
        class_names = self._target_class_names(str(dataset.get("target_mode", "triple_barrier")))
        confusion_matrix: list[list[int]] = [[0, 0, 0] for _ in class_ids]
        for true_cls, pred_cls in zip(y_true_np.tolist(), y_pred_np.tolist()):
            if 0 <= int(true_cls) < 3 and 0 <= int(pred_cls) < 3:
                confusion_matrix[int(true_cls)][int(pred_cls)] += 1

        precision_by_class: dict[str, float] = {}
        recall_by_class: dict[str, float] = {}
        f1_by_class: dict[str, float] = {}
        for idx, cls_name in enumerate(class_names):
            tp = float(confusion_matrix[idx][idx])
            fp = float(sum(confusion_matrix[r][idx] for r in class_ids if r != idx))
            fn = float(sum(confusion_matrix[idx][c] for c in class_ids if c != idx))
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
            f1 = (2.0 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
            precision_by_class[cls_name] = float(precision)
            recall_by_class[cls_name] = float(recall)
            f1_by_class[cls_name] = float(f1)

        precision_macro = float(sum(precision_by_class.values()) / len(class_names))
        recall_macro = float(sum(recall_by_class.values()) / len(class_names))
        f1_macro = float(sum(f1_by_class.values()) / len(class_names))
        sharpe_proxy = 0.0
        if magnitude_rmse > 1e-6:
            sharpe_proxy = pnl_proxy / magnitude_rmse
        # Keep legacy down/flat/up metric keys populated for dashboards even when
        # target mode is trade_outcome (short_good/no_trade/long_good).
        down_key = class_names[0] if len(class_names) > 0 else "down"
        flat_key = class_names[1] if len(class_names) > 1 else "flat"
        up_key = class_names[2] if len(class_names) > 2 else "up"
        metrics = {
            "direction_accuracy": round(direction_acc, 6),
            "direction_precision_macro": round(precision_macro, 6),
            "direction_recall_macro": round(recall_macro, 6),
            "direction_f1_macro": round(f1_macro, 6),
            "direction_precision_down": round(float(precision_by_class.get(down_key, 0.0)), 6),
            "direction_precision_flat": round(float(precision_by_class.get(flat_key, 0.0)), 6),
            "direction_precision_up": round(float(precision_by_class.get(up_key, 0.0)), 6),
            "direction_recall_down": round(float(recall_by_class.get(down_key, 0.0)), 6),
            "direction_recall_flat": round(float(recall_by_class.get(flat_key, 0.0)), 6),
            "direction_recall_up": round(float(recall_by_class.get(up_key, 0.0)), 6),
            "direction_f1_down": round(float(f1_by_class.get(down_key, 0.0)), 6),
            "direction_f1_flat": round(float(f1_by_class.get(flat_key, 0.0)), 6),
            "direction_f1_up": round(float(f1_by_class.get(up_key, 0.0)), 6),
            "direction_confusion_matrix": confusion_matrix,
            "direction_class_order": class_names,
            "magnitude_rmse": round(magnitude_rmse, 6),
            "magnitude_rmse_pct": round(magnitude_rmse_pct, 6),
            "quality_accuracy": round(quality_acc, 6),
            "pnl_proxy": round(pnl_proxy, 6),
            "sharpe_proxy": round(sharpe_proxy, 6),
            "best_val_loss": round(float(train_result["best_val_loss"]), 6),
            "samples_test": int(n_test),
            "temperature_value": round(float(temperature_value), 6),
        }
        capture_trace_splits = self._enabled_trace_splits(config)
        for trace_split in capture_trace_splits:
            await self._capture_epoch_prediction_trace(
                run_id=run_id,
                pair_symbol=pair_symbol,
                epoch_index=0,
                split=trace_split,
                model=model,
                dataset=dataset,
                config=config,
                temperature_value=float(temperature_value),
            )
        return metrics

    def _fit_temperature_from_validation(
        self,
        *,
        model: Any,
        dataset: dict[str, Any],
        lookback_steps: int,
        eval_batch_size: int,
    ) -> float:
        try:
            import torch
        except Exception:
            return 1.0
        normalized_features_np = self._resolve_normalized_features_matrix(dataset)
        window_indices_val_np = np.asarray(dataset.get("window_indices_val", []), dtype=np.int64)
        y_dir_val_np = np.asarray(dataset.get("y_dir_val", []), dtype=np.int64)
        n_val = int(len(window_indices_val_np))
        if n_val <= 0:
            return 1.0
        model.eval()
        model_device = next(model.parameters()).device
        logits_parts: list[np.ndarray] = []
        targets_parts: list[np.ndarray] = []
        with torch.no_grad():
            for start in range(0, n_val, eval_batch_size):
                end = min(n_val, start + eval_batch_size)
                rows = window_indices_val_np[start:end]
                windows_np = np.stack(
                    [normalized_features_np[int(row_idx) - lookback_steps : int(row_idx)] for row_idx in rows],
                    axis=0,
                ).astype(np.float32, copy=False)
                x_b = torch.from_numpy(windows_np).to(device=model_device, dtype=torch.float32)
                dir_logits, _, _ = model(x_b)
                logits_parts.append(dir_logits.cpu().numpy().astype(np.float32))
                targets_parts.append(y_dir_val_np[start:end].astype(np.int64))
        if not logits_parts:
            return 1.0
        logits_np = np.concatenate(logits_parts, axis=0)
        y_np = np.concatenate(targets_parts, axis=0)
        temp_grid = [0.6, 0.8, 1.0, 1.2, 1.5, 2.0, 2.5, 3.0]
        best_temp = 1.0
        best_nll = float("inf")
        for temp in temp_grid:
            scaled = logits_np / max(1e-6, float(temp))
            scaled = scaled - np.max(scaled, axis=1, keepdims=True)
            expv = np.exp(scaled)
            probs = expv / np.maximum(1e-12, np.sum(expv, axis=1, keepdims=True))
            idx = np.arange(len(y_np))
            nll = float(-np.mean(np.log(np.maximum(1e-12, probs[idx, y_np]))))
            if nll < best_nll:
                best_nll = nll
                best_temp = float(temp)
        return float(best_temp)

    def _parse_epoch_trace_capture_list(self, raw: Any, max_epochs: int) -> list[int]:
        if max_epochs <= 0:
            return []
        values: list[int] = []
        if isinstance(raw, str):
            for part in raw.split(","):
                item = part.strip()
                if not item:
                    continue
                try:
                    values.append(int(item))
                except ValueError:
                    continue
        elif isinstance(raw, (list, tuple)):
            for item in raw:
                try:
                    values.append(int(item))
                except Exception:
                    continue
        return sorted({v for v in values if 1 <= int(v) <= int(max_epochs)})

    def _enabled_trace_splits(self, config: dict[str, Any]) -> list[str]:
        training = config.get("training", {}) if isinstance(config, dict) else {}
        out: list[str] = []
        if self._as_bool(training.get("capture_trace_train", False), False):
            out.append("train")
        if self._as_bool(training.get("capture_trace_val", False), False):
            out.append("val")
        if self._as_bool(training.get("capture_trace_test", False), False):
            out.append("test")
        return out

    def _build_prediction_trace(
        self,
        *,
        y_pred_np: Any,
        y_true_np: Any,
        y_mag_np: Any,
        y_ts_np: Any,
        y_price_np: Any,
        confidence_np: Any,
        quality_np: Any,
        gap_np: Any,
        prob_down_np: Any,
        prob_flat_np: Any,
        prob_up_np: Any,
        magnitude_pred_np: Any,
        confidence_threshold: float,
        quality_threshold: float,
        min_confidence_gap: float,
        gate_profile: str,
        gate_mode: str = "confidence_only",
        entry_confidence_threshold: float | None = None,
        exit_confidence_threshold: float | None = None,
        minimum_directional_edge: float = 0.03,
        entropy_np: Any | None = None,
        epoch_index: int,
        target_mode: str = "triple_barrier",
    ) -> list[dict[str, Any]]:
        class_names = self._target_class_names(target_mode)
        n = int(len(y_pred_np))
        # Capture full split trace so replay can analyze per-candle behavior
        # across train/val/test without silent truncation.
        max_trace = n
        trace: list[dict[str, Any]] = []
        for idx in range(max_trace):
            pred_idx = int(y_pred_np[idx])
            true_idx = int(y_true_np[idx])
            conf_value = float(confidence_np[idx])
            quality_value = float(quality_np[idx])
            gap_value = float(gap_np[idx])
            entry_allowed, action, gate_flag = self._evaluate_decision(
                pred_idx=pred_idx,
                conf=conf_value,
                qual=quality_value,
                gap=gap_value,
                gate_mode=gate_mode,
                confidence_threshold=confidence_threshold,
                quality_threshold=quality_threshold,
                min_confidence_gap=min_confidence_gap,
                gate_profile=gate_profile,
                target_mode=target_mode,
            )
            blocked_reason = "" if entry_allowed else str(gate_flag)
            gate_result = "pass" if entry_allowed else "blocked"
            # y_mag is signed fractional return; convert to percentage for replay display.
            actual_future_return_pct = float(y_mag_np[idx]) * 100.0
            pred_mag_pct = float(magnitude_pred_np[idx]) * 100.0
            entropy_value = (
                float(entropy_np[idx])
                if entropy_np is not None and idx < int(len(entropy_np))
                else float(
                    -(
                        float(prob_down_np[idx]) * np.log(max(float(prob_down_np[idx]), 1e-12))
                        + float(prob_flat_np[idx]) * np.log(max(float(prob_flat_np[idx]), 1e-12))
                        + float(prob_up_np[idx]) * np.log(max(float(prob_up_np[idx]), 1e-12))
                    )
                )
            )
            trace.append(
                {
                    "epoch": int(epoch_index),
                    "sample_index": int(idx),
                    "ts_key": int(idx),
                    "ts_ms": int(y_ts_np[idx]) if idx < len(y_ts_np) else int(idx),
                    "price_at_prediction": round(float(y_price_np[idx]) if idx < len(y_price_np) else 0.0, 8),
                    "predicted_label": class_names[pred_idx] if 0 <= pred_idx < 3 else "unknown",
                    "action": action,
                    "actual_label": class_names[true_idx] if 0 <= true_idx < 3 else "unknown",
                    "prob_down": round(float(prob_down_np[idx]), 6),
                    "prob_flat": round(float(prob_flat_np[idx]), 6),
                    "prob_up": round(float(prob_up_np[idx]), 6),
                    "confidence": round(conf_value, 6),
                    "confidence_gap": round(gap_value, 6),
                    "prob_gap": round(gap_value, 6),
                    "entropy": round(entropy_value, 6),
                    "quality": round(quality_value, 6),
            "predicted_magnitude_pct": round(pred_mag_pct, 6),
            "predicted_abs_move_pct": round(abs(pred_mag_pct), 6),
            "magnitude_pct": round(pred_mag_pct, 6),
                    "actual_future_return_pct": round(actual_future_return_pct, 6),
                    "entry_allowed": bool(entry_allowed),
                    "blocked_reason": blocked_reason,
                    "gate_result": gate_result,
                    "correct": bool(pred_idx == true_idx),
                }
            )
        return trace

    def _evaluate_decision(
        self,
        *,
        pred_idx: int,
        conf: float,
        qual: float,
        gap: float,
        gate_mode: str,
        confidence_threshold: float,
        quality_threshold: float,
        min_confidence_gap: float,
        gate_profile: str,
        target_mode: str = "triple_barrier",
    ) -> tuple[bool, str, str]:
        blocked_reason = ""
        mode_target = str(target_mode or "triple_barrier").strip().lower()
        short_idx = 0
        flat_idx = 1
        long_idx = 2
        entry_allowed = True
        if pred_idx == flat_idx:
            entry_allowed = False
            blocked_reason = "predicted_flat"
        else:
            mode = str(gate_mode or "confidence_only").strip().lower()
            if mode == "combined":
                q_thr = quality_threshold
                c_thr = confidence_threshold
                g_thr = min_confidence_gap
                profile = str(gate_profile or "balanced").strip().lower()
                if profile == "balanced":
                    q_thr = max(0.0, quality_threshold - 0.10)
                    c_thr = max(0.0, confidence_threshold - 0.10)
                    g_thr = max(0.0, min_confidence_gap - 0.02)
                elif profile == "permissive":
                    q_thr = max(0.0, quality_threshold - 0.20)
                    c_thr = max(0.0, confidence_threshold - 0.20)
                    g_thr = max(0.0, min_confidence_gap - 0.04)
                if qual < q_thr:
                    entry_allowed = False
                    blocked_reason = "quality_below_threshold"
                elif conf < c_thr:
                    entry_allowed = False
                    blocked_reason = "confidence_below_threshold"
                elif gap < g_thr:
                    entry_allowed = False
                    blocked_reason = "confidence_gap_too_low"
            elif conf < confidence_threshold:
                entry_allowed = False
                blocked_reason = "confidence_below_threshold"
        action = "HOLD"
        if entry_allowed:
            if pred_idx == long_idx:
                action = "LONG"
            elif pred_idx == short_idx:
                action = "SHORT"
            else:
                entry_allowed = False
                blocked_reason = "other_gate"
        gate_result = "pass" if entry_allowed else "blocked"
        return bool(entry_allowed), action, gate_result if entry_allowed else blocked_reason

    async def _capture_epoch_prediction_trace(
        self,
        *,
        run_id: str,
        pair_symbol: str,
        epoch_index: int,
        split: str,
        model: Any,
        dataset: dict[str, Any],
        config: dict[str, Any],
        temperature_value: float = 1.0,
    ) -> None:
        try:
            import torch
        except Exception:
            return
        split_value = str(split or "test").strip().lower()
        if split_value not in {"train", "val", "test"}:
            split_value = "test"
        target_mode_value = str(dataset.get("target_mode", "triple_barrier") or "triple_barrier").strip().lower()
        normalized_features_np = self._resolve_normalized_features_matrix(dataset)
        lookback_steps = int(dataset.get("lookback_steps", 0) or 0)
        window_indices_np = np.asarray(dataset.get(f"window_indices_{split_value}", []), dtype=np.int64)
        y_dir_np = np.asarray(dataset.get(f"y_dir_{split_value}", []), dtype=np.int64)
        y_mag_np = np.asarray(dataset.get(f"y_mag_{split_value}", []), dtype=np.float32)
        y_ts_np = np.asarray(dataset.get(f"ts_{split_value}", np.arange(len(y_dir_np), dtype=np.int64)), dtype=np.int64)
        y_price_np = np.asarray(dataset.get(f"price_{split_value}", np.zeros(len(y_dir_np), dtype=np.float32)), dtype=np.float32)
        y_dir_train_np = np.asarray(dataset.get("y_dir_train", []), dtype=np.int64)
        y_dir_val_np = np.asarray(dataset.get("y_dir_val", []), dtype=np.int64)
        y_dir_test_np = np.asarray(dataset.get("y_dir_test", []), dtype=np.int64)
        stale_train_np = np.asarray(dataset.get("stale_mask_train", []), dtype=np.bool_)
        stale_val_np = np.asarray(dataset.get("stale_mask_val", []), dtype=np.bool_)
        stale_test_np = np.asarray(dataset.get("stale_mask_test", []), dtype=np.bool_)
        n_test = int(len(window_indices_np))
        if n_test <= 0:
            return
        eval_batch_size = max(128, min(int(config["training"].get("batch_size", 64)) * 8, 4096))
        confidence_threshold = float(config["paper_bot"].get("confidence_threshold", 0.5) or 0.5)
        quality_threshold = float(config["paper_bot"].get("quality_threshold", 0.5) or 0.5)
        min_confidence_gap = max(0.0, float(config["paper_bot"].get("min_confidence_gap", 0.0) or 0.0))
        gate_profile = str(config["paper_bot"].get("gate_profile", "balanced") or "balanced").strip().lower()
        gate_mode = str(config["paper_bot"].get("gate_mode", "confidence_only") or "confidence_only").strip().lower()

        y_pred_np = np.empty(n_test, dtype=np.int64)
        conf_np = np.zeros(n_test, dtype=np.float32)
        gap_np = np.zeros(n_test, dtype=np.float32)
        quality_np = np.zeros(n_test, dtype=np.float32)
        mag_np = np.zeros(n_test, dtype=np.float32)
        prob_down_np = np.zeros(n_test, dtype=np.float32)
        prob_flat_np = np.zeros(n_test, dtype=np.float32)
        prob_up_np = np.zeros(n_test, dtype=np.float32)
        entropy_np = np.zeros(n_test, dtype=np.float32)

        model.eval()
        model_device = next(model.parameters()).device
        with torch.no_grad():
            for start in range(0, n_test, eval_batch_size):
                end = min(n_test, start + eval_batch_size)
                rows = window_indices_np[start:end]
                windows_np = np.stack(
                    [
                        normalized_features_np[int(row_idx) - lookback_steps : int(row_idx)]
                        for row_idx in rows
                    ],
                    axis=0,
                ).astype(np.float32, copy=False)
                x_b = torch.from_numpy(windows_np).to(device=model_device, dtype=torch.float32)
                dir_logits, mag_out, quality_logits = model(x_b)
                probs = torch.softmax(dir_logits / max(1e-6, float(temperature_value)), dim=1)
                pred_cls = torch.argmax(probs, dim=1).cpu().numpy().astype(np.int64)
                conf = torch.max(probs, dim=1).values.cpu().numpy().astype(np.float32)
                sorted_probs = torch.sort(probs, dim=1, descending=True).values.cpu().numpy().astype(np.float32)
                quality_prob = torch.sigmoid(quality_logits).cpu().numpy().astype(np.float32)
                mag_pred = mag_out.cpu().numpy().astype(np.float32)
                y_pred_np[start:end] = pred_cls
                conf_np[start:end] = conf
                gap_np[start:end] = (sorted_probs[:, 0] - sorted_probs[:, 1]).astype(np.float32)
                quality_np[start:end] = quality_prob
                mag_np[start:end] = mag_pred
                prob_down_np[start:end] = probs[:, 0].cpu().numpy().astype(np.float32)
                prob_flat_np[start:end] = probs[:, 1].cpu().numpy().astype(np.float32)
                prob_up_np[start:end] = probs[:, 2].cpu().numpy().astype(np.float32)
                pnp = probs.cpu().numpy().astype(np.float32)
                entropy_np[start:end] = (-(pnp * np.log(np.maximum(pnp, 1e-12))).sum(axis=1)).astype(np.float32)

        trace = self._build_prediction_trace(
            y_pred_np=y_pred_np,
            y_true_np=y_dir_np,
            y_mag_np=y_mag_np,
            y_ts_np=y_ts_np,
            y_price_np=y_price_np,
            confidence_np=conf_np,
            quality_np=quality_np,
            gap_np=gap_np,
            prob_down_np=prob_down_np,
            prob_flat_np=prob_flat_np,
            prob_up_np=prob_up_np,
            magnitude_pred_np=mag_np,
            confidence_threshold=confidence_threshold,
            quality_threshold=quality_threshold,
            min_confidence_gap=min_confidence_gap,
            gate_profile=gate_profile,
            gate_mode=gate_mode,
            entropy_np=entropy_np,
            epoch_index=int(epoch_index),
            target_mode=target_mode_value,
        )

        # Trace diagnostics summary for replay + run metrics.
        total = max(1, len(trace))
        action_counts = {"LONG": 0, "SHORT": 0, "HOLD": 0}
        blocked_reasons: dict[str, int] = {}
        non_flat = 0
        non_flat_trades = 0
        directional_predictions = 0
        directional_actions = 0
        long_when_up = 0
        down_total = 0
        up_total = 0
        short_when_down = 0
        false_long = 0
        false_short = 0
        for row in trace:
            action = str(row.get("action", "HOLD")).upper()
            actual = str(row.get("actual_label", "flat")).lower()
            action_counts[action] = int(action_counts.get(action, 0)) + 1
            if not bool(row.get("entry_allowed", False)):
                reason = str(row.get("blocked_reason", "") or "other_gate")
                blocked_reasons[reason] = int(blocked_reasons.get(reason, 0)) + 1
            if actual != "flat":
                non_flat += 1
            pred_lbl_for_rate = str(row.get("predicted_label", "flat") or "flat").lower()
            if pred_lbl_for_rate in {"up", "down"}:
                directional_predictions += 1
            if action in {"LONG", "SHORT"}:
                non_flat_trades += 1
                directional_actions += 1
            if actual == "up":
                up_total += 1
                if action == "LONG":
                    long_when_up += 1
            if actual == "down":
                down_total += 1
                if action == "SHORT":
                    short_when_down += 1
            if action == "LONG" and actual != "up":
                false_long += 1
            if action == "SHORT" and actual != "down":
                false_short += 1
        trade_rate_by_class = {
            "long_rate_when_actual_up": float(long_when_up / max(1, up_total)),
            "short_rate_when_actual_down": float(short_when_down / max(1, down_total)),
            "false_long_rate": float(false_long / max(1, action_counts["LONG"])),
            "false_short_rate": float(false_short / max(1, action_counts["SHORT"])),
            "actual_up_count": int(up_total),
            "actual_down_count": int(down_total),
            "pred_long_count": int(action_counts["LONG"]),
            "pred_short_count": int(action_counts["SHORT"]),
        }
        abstain_total = int(action_counts.get("HOLD", 0))
        abstain_correct = int(sum(1 for row in trace if str(row.get("action", "HOLD")).upper() == "HOLD" and str(row.get("actual_label", "flat")).lower() == "flat"))
        abstention_rate = float(abstain_total / max(1, total))
        abstention_correctness = float(abstain_correct / max(1, abstain_total))
        bucket_edges = [0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 1.01]
        bucket_stats: list[dict[str, Any]] = []
        min_bucket = max(1, int(config["paper_bot"].get("min_samples_per_bucket", 200) or 200))
        for lo, hi in zip(bucket_edges[:-1], bucket_edges[1:]):
            rows = [
                r for r in trace
                if float(r.get("confidence", 0.0) or 0.0) >= lo and float(r.get("confidence", 0.0) or 0.0) < hi
            ]
            if not rows:
                continue
            returns = sorted([float(r.get("actual_future_return_pct", 0.0) or 0.0) for r in rows])
            avg_ret = float(sum(returns) / max(1, len(returns)))
            med_ret = float(returns[len(returns) // 2])
            directional = [r for r in rows if str(r.get("predicted_label", "flat")) in {"up", "down"} and str(r.get("actual_label", "flat")) in {"up", "down"}]
            hits = sum(1 for r in directional if str(r.get("predicted_label")) == str(r.get("actual_label")))
            bucket_stats.append(
                {
                    "bucket": f"{lo:.2f}-{min(hi, 1.0):.2f}",
                    "sample_count": int(len(rows)),
                    "avg_future_return_pct": round(avg_ret, 6),
                    "median_future_return_pct": round(med_ret, 6),
                    "direction_hit_rate": round(float(hits / max(1, len(directional))), 6),
                    "low_confidence_bucket": bool(len(rows) < min_bucket),
                }
            )
        # Confidence histogram + spread diagnostics.
        hist_bins: list[tuple[float, float, str]] = [
            (0.33, 0.40, "0.33-0.40"),
            (0.40, 0.50, "0.40-0.50"),
            (0.50, 0.60, "0.50-0.60"),
            (0.60, 1.01, "0.60+"),
        ]
        confidence_distribution_histogram: list[dict[str, Any]] = []
        for lo, hi, label in hist_bins:
            rows_bin = [r for r in trace if float(r.get("confidence", 0.0) or 0.0) >= lo and float(r.get("confidence", 0.0) or 0.0) < hi]
            pct = float(len(rows_bin) / max(1, len(trace)))
            confidence_distribution_histogram.append(
                {
                    "bucket": label,
                    "count": int(len(rows_bin)),
                    "pct": round(pct, 6),
                }
            )
        top1_arr = np.asarray([float(r.get("confidence", 0.0) or 0.0) for r in trace], dtype=np.float32)
        top2_arr = np.asarray([max(0.0, float(r.get("confidence", 0.0) or 0.0) - float(r.get("confidence_gap", 0.0) or 0.0)) for r in trace], dtype=np.float32)
        gap_arr = np.asarray([float(r.get("confidence_gap", 0.0) or 0.0) for r in trace], dtype=np.float32)
        confidence_spread_stats = {
            "mean_top1_prob": float(np.mean(top1_arr)) if top1_arr.size else 0.0,
            "mean_top2_prob": float(np.mean(top2_arr)) if top2_arr.size else 0.0,
            "mean_confidence_gap": float(np.mean(gap_arr)) if gap_arr.size else 0.0,
        }
        # Future-return distribution by actual label quality check.
        label_rows: dict[str, list[float]] = {"up": [], "flat": [], "down": []}
        for r in trace:
            lbl = str(r.get("actual_label", "flat") or "flat").lower()
            if lbl not in label_rows:
                continue
            label_rows[lbl].append(float(r.get("actual_future_return_pct", 0.0) or 0.0))

        def _dist(values: list[float]) -> dict[str, Any]:
            if not values:
                return {"count": 0, "mean": 0.0, "median": 0.0, "p25": 0.0, "p75": 0.0, "min": 0.0, "max": 0.0}
            arr = np.asarray(values, dtype=np.float64)
            return {
                "count": int(arr.size),
                "mean": float(np.mean(arr)),
                "median": float(np.median(arr)),
                "p25": float(np.percentile(arr, 25)),
                "p75": float(np.percentile(arr, 75)),
                "min": float(np.min(arr)),
                "max": float(np.max(arr)),
            }

        future_return_distribution_by_label = {
            "up": _dist(label_rows["up"]),
            "flat": _dist(label_rows["flat"]),
            "down": _dist(label_rows["down"]),
        }
        # Feature separability diagnostics on current split windows.
        feature_separability: dict[str, Any] = {}
        feature_coverage: dict[str, Any] = {}
        source_health: dict[str, Any] = {}
        if normalized_features_np.ndim == 2 and normalized_features_np.shape[0] > 0 and normalized_features_np.shape[1] > 0:
            # Use the last row of each lookback window as the decision-time feature vector.
            feature_anchor_idx = np.clip(window_indices_np - 1, 0, max(0, normalized_features_np.shape[0] - 1))
            split_feat_matrix = normalized_features_np[feature_anchor_idx]
            y_true_labels = np.asarray(y_dir_np[: split_feat_matrix.shape[0]], dtype=np.int64)
            future_returns_pct = np.asarray(y_mag_np[: split_feat_matrix.shape[0]], dtype=np.float32) * 100.0
            label_masks = {
                "down": (y_true_labels == 0),
                "flat": (y_true_labels == 1),
                "up": (y_true_labels == 2),
            }
            for feat_idx, feat_name in enumerate(FEATURE_COLUMNS):
                if feat_idx >= split_feat_matrix.shape[1]:
                    break
                vals = split_feat_matrix[:, feat_idx].astype(np.float64, copy=False)
                zero_rate = float(np.mean(np.isclose(vals, 0.0))) if vals.size else 1.0
                feature_coverage[feat_name] = {
                    "count": int(vals.size),
                    "non_null_count": int(vals.size),
                    "zero_rate": zero_rate,
                    "min": float(np.min(vals)) if vals.size else 0.0,
                    "max": float(np.max(vals)) if vals.size else 0.0,
                    "mean": float(np.mean(vals)) if vals.size else 0.0,
                    "std": float(np.std(vals)) if vals.size else 0.0,
                }
                if feat_name in {"aggressive_buy_sell_delta", "open_interest_change_pct", "oi_velocity", "cancel_rate_orderbook", "liq_events_60s", "liq_count_60s", "liq_notional_60s", "liq_buy_sell_imbalance", "liq_momentum"} and zero_rate >= 0.98:
                    await self._emit_run_log(run_id, "WARNING", f"Feature zero-rate high ({split_value}) {feat_name}={zero_rate:.3f}")
                stats_by_label: dict[str, Any] = {}
                for lbl in ("up", "flat", "down"):
                    mask = label_masks[lbl]
                    if not np.any(mask):
                        stats_by_label[lbl] = {"mean": None, "std": None, "count": 0}
                        continue
                    arr = vals[mask]
                    stats_by_label[lbl] = {
                        "mean": float(np.mean(arr)),
                        "std": float(np.std(arr)),
                        "count": int(arr.size),
                    }

                def _pair_sep(a: str, b: str) -> float | None:
                    am = stats_by_label.get(a, {})
                    bm = stats_by_label.get(b, {})
                    if am.get("mean") is None or bm.get("mean") is None:
                        return None
                    std_a = float(am.get("std") or 0.0)
                    std_b = float(bm.get("std") or 0.0)
                    pooled = float(np.sqrt((std_a * std_a + std_b * std_b) / 2.0))
                    if pooled <= 1e-12:
                        return None
                    return float(abs(float(am["mean"]) - float(bm["mean"])) / pooled)

                corr_with_future_return: float | None = None
                if vals.size > 1 and np.std(vals) > 1e-12 and np.std(future_returns_pct) > 1e-12:
                    corr_mat = np.corrcoef(vals, future_returns_pct)
                    corr_val = float(corr_mat[0, 1]) if corr_mat.shape == (2, 2) else float("nan")
                    corr_with_future_return = corr_val if np.isfinite(corr_val) else None

                feature_separability[feat_name] = {
                    "by_label": stats_by_label,
                    "separability": {
                        "up_flat": _pair_sep("up", "flat"),
                        "up_down": _pair_sep("up", "down"),
                        "flat_down": _pair_sep("flat", "down"),
                    },
                    "corr_with_future_return": corr_with_future_return,
                }
            ranked = sorted(
                (
                    (
                        name,
                        max(
                            [float(v) for v in (data.get("separability", {}) or {}).values() if isinstance(v, (float, int))],
                            default=0.0,
                        ),
                    )
                    for name, data in feature_separability.items()
                ),
                key=lambda x: x[1],
                reverse=True,
            )
            if ranked:
                preview = ", ".join(f"{name}:{score:.3f}" for name, score in ranked[:3])
                await self._emit_run_log(run_id, "INFO", f"Feature separability top3 ({split_value}): {preview}")
            for key in ("open_interest_change_pct", "oi_velocity", "liq_events_60s", "liq_count_60s", "liq_notional_60s", "liq_buy_sell_imbalance", "liq_momentum"):
                cov = feature_coverage.get(key, {})
                zero_rate = float(cov.get("zero_rate", 1.0) or 1.0)
                status = "healthy"
                if zero_rate >= 0.999:
                    status = "source_absent_or_missing"
                elif zero_rate >= 0.98:
                    status = "mostly_zero"
                source_health[key] = {"zero_rate": zero_rate, "status": status}

        entry_allowed = sum(1 for r in trace if bool(r.get("entry_allowed", False)))
        gate_paralyzed = bool(entry_allowed == 0 and any(str(r.get("predicted_label", "flat")) in {"up", "down"} for r in trace))
        if gate_paralyzed:
            await self._emit_run_log(run_id, "WARNING", f"Gate paralyzed on trace split={split_value} epoch={epoch_index}: entry_allowed_rate=0.")
        decision_hash_payload = [
            {
                "ts_ms": int(r.get("ts_ms", 0) or 0),
                "predicted_label": str(r.get("predicted_label", "")),
                "action": str(r.get("action", "HOLD")),
                "entry_allowed": bool(r.get("entry_allowed", False)),
                "blocked_reason": str(r.get("blocked_reason", "")),
                "confidence": float(r.get("confidence", 0.0) or 0.0),
                "prob_gap": float(r.get("prob_gap", r.get("confidence_gap", 0.0)) or 0.0),
                "entropy": float(r.get("entropy", 0.0) or 0.0),
            }
            for r in trace
        ]
        decision_trace_hash = hashlib.sha256(
            json.dumps(decision_hash_payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        ).hexdigest()
        def _label_dist(arr: np.ndarray, mode: str) -> dict[str, Any]:
            if arr.size <= 0:
                return {}
            if mode == "trade_outcome":
                labels = ["short_good", "no_trade", "long_good"]
            else:
                labels = ["down", "flat", "up"]
            out: dict[str, Any] = {}
            n = max(1, int(arr.size))
            for i, lbl in enumerate(labels):
                c = int(np.sum(arr == i))
                out[lbl] = {"count": c, "pct": float(c / n)}
            return out
        split_label_distribution = {
            "train": _label_dist(y_dir_train_np, target_mode_value),
            "val": _label_dist(y_dir_val_np, target_mode_value),
            "test": _label_dist(y_dir_test_np, target_mode_value),
        }
        split_stale_rates = {
            "train": float(np.mean(stale_train_np)) if stale_train_np.size else 0.0,
            "val": float(np.mean(stale_val_np)) if stale_val_np.size else 0.0,
            "test": float(np.mean(stale_test_np)) if stale_test_np.size else 0.0,
        }
        # Threshold calibration sweep (validation split only).
        calibration_updates: dict[str, Any] = {}
        if split_value == "val":
            threshold_grid = [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]
            best_score = float("-inf")
            best_row: dict[str, Any] | None = None
            candidates = 0
            min_action_rate = max(0.0, min(1.0, float(config["paper_bot"].get("minimum_action_rate", 0.03) or 0.03)))
            min_directional_samples = max(1, int(float(config["paper_bot"].get("minimum_directional_samples", 30) or 30)))
            action_rate_rejections = 0
            directional_rejections = 0
            fee_bps_rt = max(0.0, float(config["paper_bot"].get("fee_bps_round_trip", 4.0) or 4.0))
            slippage_bps_rt = max(0.0, float(config["paper_bot"].get("slippage_bps_round_trip", 2.0) or 2.0))
            estimated_cost_bps = max(0.0, float(config["paper_bot"].get("estimated_roundtrip_cost_bps", fee_bps_rt + slippage_bps_rt) or (fee_bps_rt + slippage_bps_rt)))
            cost_pct = estimated_cost_bps / 10000.0 * 100.0
            sweep_rows: list[dict[str, Any]] = []
            regime_rows: list[dict[str, Any]] = []
            for c_thr in threshold_grid:
                candidates += 1
                tp = fp = fn = 0
                up_tp = up_fp = down_tp = down_fp = 0
                long_n = short_n = hold_n = 0
                directional_tp = directional_fp = directional_fn = 0
                directional_samples = 0
                pnl_after_fee = 0.0
                returns_for_sharpe: list[float] = []
                prob_gaps: list[float] = []
                entropies: list[float] = []
                conf_all: list[float] = []
                correct_all: list[float] = []
                brier_terms: list[float] = []
                for r in trace:
                    pred_lbl = str(r.get("predicted_label", "flat"))
                    act_lbl = str(r.get("actual_label", "flat"))
                    conf = float(r.get("confidence", 0.0) or 0.0)
                    conf_all.append(conf)
                    correct_all.append(1.0 if pred_lbl == act_lbl else 0.0)
                    pd = float(r.get("prob_down", 0.0) or 0.0)
                    pf = float(r.get("prob_flat", 0.0) or 0.0)
                    pu = float(r.get("prob_up", 0.0) or 0.0)
                    y_down = 1.0 if act_lbl == "down" else 0.0
                    y_flat = 1.0 if act_lbl == "flat" else 0.0
                    y_up = 1.0 if act_lbl == "up" else 0.0
                    brier_terms.append(((pd - y_down) ** 2 + (pf - y_flat) ** 2 + (pu - y_up) ** 2) / 3.0)
                    if pred_lbl == "flat" or conf < c_thr:
                        hold_n += 1
                        continue
                    if pred_lbl == "up":
                        long_n += 1
                    elif pred_lbl == "down":
                        short_n += 1
                    prob_gaps.append(float(r.get("prob_gap", r.get("confidence_gap", 0.0)) or 0.0))
                    entropies.append(float(r.get("entropy", 0.0) or 0.0))
                    move_pct = float(r.get("actual_future_return_pct", 0.0) or 0.0)
                    signed = move_pct if pred_lbl == "up" else (-move_pct if pred_lbl == "down" else 0.0)
                    ret = signed - cost_pct
                    pnl_after_fee += ret
                    returns_for_sharpe.append(ret)
                    if pred_lbl == "up":
                        if act_lbl == "up":
                            up_tp += 1
                        else:
                            up_fp += 1
                    elif pred_lbl == "down":
                        if act_lbl == "down":
                            down_tp += 1
                        else:
                            down_fp += 1
                    if (pred_lbl == "up" and act_lbl == "up") or (pred_lbl == "down" and act_lbl == "down"):
                        tp += 1
                    else:
                        fp += 1
                    if act_lbl in {"up", "down"} and pred_lbl in {"up", "down"}:
                        directional_samples += 1
                        if pred_lbl == act_lbl:
                            directional_tp += 1
                        else:
                            directional_fp += 1
                            directional_fn += 1
                    elif act_lbl in {"up", "down"} and pred_lbl == "flat":
                        fn += 1

                trades = long_n + short_n
                total_rows = max(1, len(trace))
                action_rate = float(trades / total_rows)
                hold_rate = float(hold_n / total_rows)
                long_rate = float(long_n / total_rows)
                short_rate = float(short_n / total_rows)
                precision = float(tp / max(1, tp + fp))
                precision_up = float(up_tp / max(1, up_tp + up_fp))
                precision_down = float(down_tp / max(1, down_tp + down_fp))
                recall = float(tp / max(1, tp + fn))
                f1 = float((2 * precision * recall) / max(1e-9, precision + recall))
                pnl_proxy_after_fee = float(pnl_after_fee / max(1, trades))
                sharpe_proxy_after_fee = float(np.mean(returns_for_sharpe) / max(1e-9, np.std(returns_for_sharpe))) if len(returns_for_sharpe) > 1 else 0.0
                precision_directional = float(directional_tp / max(1, directional_tp + directional_fp))
                recall_directional = float(directional_tp / max(1, directional_tp + directional_fn))
                f1_directional = float((2 * precision_directional * recall_directional) / max(1e-9, precision_directional + recall_directional))
                # ECE on confidence (equal-width bins).
                ece = 0.0
                if conf_all:
                    conf_arr = np.asarray(conf_all, dtype=np.float32)
                    corr_arr = np.asarray(correct_all, dtype=np.float32)
                    bins = np.linspace(0.0, 1.0, 11, dtype=np.float32)
                    for bi in range(10):
                        lo = float(bins[bi])
                        hi = float(bins[bi + 1])
                        if bi == 9:
                            mask = (conf_arr >= lo) & (conf_arr <= hi)
                        else:
                            mask = (conf_arr >= lo) & (conf_arr < hi)
                        nbin = int(np.sum(mask))
                        if nbin <= 0:
                            continue
                        acc_bin = float(np.mean(corr_arr[mask]))
                        conf_bin = float(np.mean(conf_arr[mask]))
                        ece += abs(acc_bin - conf_bin) * (nbin / max(1, len(conf_all)))
                brier = float(np.mean(np.asarray(brier_terms, dtype=np.float32))) if brier_terms else 0.0
                row = {
                    "threshold": float(c_thr),
                    "action_rate": action_rate,
                    "long_rate": long_rate,
                    "short_rate": short_rate,
                    "hold_rate": hold_rate,
                    "macro_f1": f1,
                    "precision_up": precision_up,
                    "precision_down": precision_down,
                    "precision_directional": precision_directional,
                    "recall_directional": recall_directional,
                    "f1_directional": f1_directional,
                    "mean_prob_gap": float(np.mean(prob_gaps)) if prob_gaps else 0.0,
                    "mean_entropy": float(np.mean(entropies)) if entropies else 0.0,
                    "ece_score": float(ece),
                    "brier_score": float(brier),
                    "pnl_proxy_after_cost": pnl_proxy_after_fee,
                    "sharpe_proxy_after_cost": sharpe_proxy_after_fee,
                    "directional_samples": int(directional_samples),
                    "valid": True,
                    "rejection_reasons": [],
                }
                if action_rate < min_action_rate:
                    row["valid"] = False
                    row["rejection_reasons"].append("below_minimum_action_rate")
                    action_rate_rejections += 1
                if directional_samples < min_directional_samples:
                    row["valid"] = False
                    row["rejection_reasons"].append("below_minimum_directional_samples")
                    directional_rejections += 1
                sweep_rows.append(row)

                if not row["valid"]:
                    continue
                in_band_bonus = 1.0 if 0.05 <= action_rate <= 0.15 else 0.0
                score = (
                    in_band_bonus * 10.0
                    + f1_directional * 5.0
                    + pnl_proxy_after_fee * 2.0
                    + sharpe_proxy_after_fee * 1.0
                    + c_thr * 0.001
                )
                if score > best_score:
                    best_score = score
                    best_row = dict(row)

                # Regime analytics snapshot per threshold (lightweight segmentation).
                def _session_bucket(ts_ms: int) -> str:
                    try:
                        h = datetime.fromtimestamp(max(0, ts_ms) / 1000.0, tz=timezone.utc).hour
                    except Exception:
                        h = 0
                    if 0 <= h < 8:
                        return "asia"
                    if 8 <= h < 16:
                        return "eu"
                    return "us"
                conf_vals: list[float] = []
                ts_vals: list[int] = []
                ret_vals: list[float] = []
                for rr in trace:
                    conf_vals.append(float(rr.get("confidence", 0.0) or 0.0))
                    ts_vals.append(int(rr.get("ts_ms", 0) or 0))
                    ret_vals.append(float(rr.get("actual_future_return_pct", 0.0) or 0.0))
                if conf_vals:
                    vols = np.asarray([abs(v) for v in ret_vals], dtype=np.float32)
                    q1, q2, q3 = np.quantile(vols, [0.25, 0.50, 0.75]).tolist()
                    vol_buckets = {"q1": [], "q2": [], "q3": [], "q4": []}
                    sess_buckets = {"asia": [], "eu": [], "us": []}
                    trend_buckets = {"trend": [], "range": []}
                    for i, cv in enumerate(conf_vals):
                        vv = float(vols[i])
                        if vv <= q1:
                            vol_buckets["q1"].append(cv)
                        elif vv <= q2:
                            vol_buckets["q2"].append(cv)
                        elif vv <= q3:
                            vol_buckets["q3"].append(cv)
                        else:
                            vol_buckets["q4"].append(cv)
                        sess_buckets[_session_bucket(ts_vals[i])].append(cv)
                        trend_buckets["trend" if abs(ret_vals[i]) >= float(q3) else "range"].append(cv)
                    regime_rows.append(
                        {
                            "threshold": float(c_thr),
                            "mean_confidence_by_regime": {
                                "volatility_quartile": {k: (float(np.mean(v)) if v else 0.0) for k, v in vol_buckets.items()},
                                "session": {k: (float(np.mean(v)) if v else 0.0) for k, v in sess_buckets.items()},
                                "trend_range": {k: (float(np.mean(v)) if v else 0.0) for k, v in trend_buckets.items()},
                            },
                        }
                    )

            if best_row is None:
                calibration_updates = {
                    "calibration_sweep_ran": False,
                    "calibration_skip_reason": "no candidate met action/directional constraints",
                    "calibration_candidate_count": int(candidates),
                    "calibration_best_score": None,
                    "confidence_threshold_sweep": sweep_rows,
                    "minimum_action_rate_used": float(min_action_rate),
                    "minimum_directional_samples_used": int(min_directional_samples),
                    "action_rate_rejections_count": int(action_rate_rejections),
                    "directional_rejections_count": int(directional_rejections),
                    "cost_basis": "round_trip_total",
                    "estimated_roundtrip_cost_bps_used": float(estimated_cost_bps),
                    "fee_bps_round_trip_used": float(fee_bps_rt),
                    "slippage_bps_round_trip_used": float(slippage_bps_rt),
                    "regime_metrics": regime_rows,
                }
            else:
                calibration_updates = {
                    "calibration_sweep_ran": True,
                    "calibration_candidate_count": int(candidates),
                    "calibration_best_score": float(best_score),
                    "confidence_threshold_sweep": sweep_rows,
                    "recommended_confidence_threshold": float(best_row["threshold"]),
                    "recommended_trade_rate": float(best_row["action_rate"]),
                    "recommended_pnl_after_fee": float(best_row["pnl_proxy_after_cost"]),
                    "minimum_action_rate_used": float(min_action_rate),
                    "minimum_directional_samples_used": int(min_directional_samples),
                    "action_rate_rejections_count": int(action_rate_rejections),
                    "directional_rejections_count": int(directional_rejections),
                    "cost_basis": "round_trip_total",
                    "estimated_roundtrip_cost_bps_used": float(estimated_cost_bps),
                    "fee_bps_round_trip_used": float(fee_bps_rt),
                    "slippage_bps_round_trip_used": float(slippage_bps_rt),
                    "regime_metrics": regime_rows,
                }
            # Label-threshold recommendation sweep using current future-return distribution.
            label_candidates = [0.03, 0.04, 0.05, 0.06, 0.08, 0.10, 0.12]
            best_lbl_thr = None
            best_lbl_score = float("-inf")
            label_sweep_rows: list[dict[str, Any]] = []
            future_returns = np.asarray([float(r.get("actual_future_return_pct", 0.0) or 0.0) / 100.0 for r in trace], dtype=np.float32)
            pred_labels = np.asarray([str(r.get("predicted_label", "flat") or "flat").lower() for r in trace], dtype=object)
            class_to_idx = {"down": 0, "flat": 1, "up": 2}
            pred_idx = np.asarray([class_to_idx.get(str(v), 1) for v in pred_labels], dtype=np.int64)
            for thr_pct in label_candidates:
                thr = float(thr_pct) / 100.0
                true_idx = np.where(future_returns > thr, 2, np.where(future_returns < -thr, 0, 1)).astype(np.int64)
                counts = np.bincount(true_idx, minlength=3)
                # macro-f1 proxy against current predictions
                f1s: list[float] = []
                for cls in (0, 1, 2):
                    tp = float(np.sum((pred_idx == cls) & (true_idx == cls)))
                    fp = float(np.sum((pred_idx == cls) & (true_idx != cls)))
                    fn = float(np.sum((pred_idx != cls) & (true_idx == cls)))
                    p = tp / (tp + fp) if (tp + fp) > 0 else 0.0
                    r = tp / (tp + fn) if (tp + fn) > 0 else 0.0
                    f1 = (2.0 * p * r) / (p + r) if (p + r) > 0 else 0.0
                    f1s.append(float(f1))
                macro_f1_proxy = float(sum(f1s) / 3.0)
                false_long_rate = float(np.sum((pred_idx == 2) & (true_idx != 2)) / max(1, np.sum(pred_idx == 2)))
                false_short_rate = float(np.sum((pred_idx == 0) & (true_idx != 0)) / max(1, np.sum(pred_idx == 0)))
                score = macro_f1_proxy - 0.10 * (false_long_rate + false_short_rate)
                if score > best_lbl_score:
                    best_lbl_score = score
                    best_lbl_thr = float(thr_pct)
                label_sweep_rows.append(
                    {
                        "label_threshold_pct": float(thr_pct),
                        "class_balance": {
                            "down": int(counts[0]),
                            "flat": int(counts[1]),
                            "up": int(counts[2]),
                        },
                        "macro_f1_proxy": round(macro_f1_proxy, 6),
                        "false_long_rate": round(false_long_rate, 6),
                        "false_short_rate": round(false_short_rate, 6),
                    }
                )
            calibration_updates["label_threshold_sweep"] = label_sweep_rows
            if best_lbl_thr is not None:
                calibration_updates["recommended_label_threshold_pct"] = float(best_lbl_thr)
                await self._emit_run_log(
                    run_id,
                    "INFO",
                    f"Label threshold sweep ({split_value}): current={float(config.get('label_threshold_pct', 0.0)):.4f}% recommended={float(best_lbl_thr):.4f}% (manual apply)",
                )
        elif split_value == "test":
            # Only report skip if no val recommendation has ever been written for this run.
            existing = await self._repository.get_run(run_id)
            exm = dict(existing.get("metrics", {}) or {}) if isinstance(existing, dict) else {}
            target_mode_used = target_mode_value
            if target_mode_used == "trade_outcome":
                thr = float(exm.get("recommended_confidence_threshold", 0.70) or 0.70)
                thr = max(0.0, min(1.0, thr))
                min_action_rate = max(0.0, min(1.0, float(config["paper_bot"].get("minimum_action_rate", 0.03) or 0.03))
                )
                min_directional_samples_cfg = max(1, int(float(config["paper_bot"].get("minimum_directional_samples", 30) or 30)))
                min_directional_samples = max(min_directional_samples_cfg, max(300, int(max(1, len(trace)) * 0.01)))
                max_false_long_rate = max(0.0, min(1.0, float(config["paper_bot"].get("max_false_long_rate", 0.65) or 0.65)))
                max_false_short_rate = max(0.0, min(1.0, float(config["paper_bot"].get("max_false_short_rate", 0.65) or 0.65)))
                min_long_precision = max(0.0, min(1.0, float(config["paper_bot"].get("min_long_precision", 0.35) or 0.35)))
                min_short_precision = max(0.0, min(1.0, float(config["paper_bot"].get("min_short_precision", 0.35) or 0.35)))
                fee_bps_rt = max(0.0, float(config["paper_bot"].get("fee_bps_round_trip", 4.0) or 4.0))
                slippage_bps_rt = max(0.0, float(config["paper_bot"].get("slippage_bps_round_trip", 2.0) or 2.0))
                estimated_cost_bps = max(0.0, float(config["paper_bot"].get("estimated_roundtrip_cost_bps", fee_bps_rt + slippage_bps_rt) or (fee_bps_rt + slippage_bps_rt)))
                cost_pct = estimated_cost_bps / 10000.0 * 100.0
                margin_grid = [0.05, 0.10, 0.15, 0.20, 0.25]
                topk_grid = [0.005, 0.01, 0.02, 0.05, 0.10]
                min_ev_threshold = float(config["paper_bot"].get("min_ev_threshold_pct", 0.0) or 0.0)
                long_wins = [float(r.get("actual_future_return_pct", 0.0) or 0.0) - cost_pct for r in trace if str(r.get("actual_label", "no_trade")) == "long_good"]
                short_wins = [(-float(r.get("actual_future_return_pct", 0.0) or 0.0)) - cost_pct for r in trace if str(r.get("actual_label", "no_trade")) == "short_good"]
                avg_true_long_good_ret = float(np.mean(long_wins)) if long_wins else 0.0
                avg_true_short_good_ret = float(np.mean(short_wins)) if short_wins else 0.0
                bad_long_rows = [
                    (float(r.get("actual_future_return_pct", 0.0) or 0.0) - cost_pct)
                    for r in trace
                    if str(r.get("predicted_label", "no_trade") or "no_trade").lower() == "long_good"
                    and str(r.get("actual_label", "no_trade") or "no_trade").lower() != "long_good"
                ]
                bad_short_rows = [
                    ((-float(r.get("actual_future_return_pct", 0.0) or 0.0)) - cost_pct)
                    for r in trace
                    if str(r.get("predicted_label", "no_trade") or "no_trade").lower() == "short_good"
                    and str(r.get("actual_label", "no_trade") or "no_trade").lower() != "short_good"
                ]
                avg_bad_long_ret = float(np.mean(bad_long_rows)) if bad_long_rows else (-abs(cost_pct))
                avg_bad_short_ret = float(np.mean(bad_short_rows)) if bad_short_rows else (-abs(cost_pct))
                ev_rows: list[dict[str, Any]] = []
                margin_sweep_rows: list[dict[str, Any]] = []
                topk_eval_rows: list[dict[str, Any]] = []
                confidence_buckets = [(0.40, 0.50), (0.50, 0.60), (0.60, 0.70), (0.70, 0.80), (0.80, 0.90), (0.90, 1.01)]
                bucket_eval_rows: list[dict[str, Any]] = []
                directional_samples = 0
                long_total = short_total = 0
                true_long = true_short = 0
                pnl_sum = 0.0
                rets: list[float] = []
                for r in trace:
                    pred_lbl = str(r.get("predicted_label", "no_trade") or "no_trade").lower()
                    act_lbl = str(r.get("actual_label", "no_trade") or "no_trade").lower()
                    conf = float(r.get("confidence", 0.0) or 0.0)
                    p_long = float(r.get("prob_up", 0.0) or 0.0)
                    p_short = float(r.get("prob_down", 0.0) or 0.0)
                    p_hold = float(r.get("prob_flat", 0.0) or 0.0)
                    ev_long = (p_long * avg_true_long_good_ret) + ((p_short + p_hold) * avg_bad_long_ret)
                    ev_short = (p_short * avg_true_short_good_ret) + ((p_long + p_hold) * avg_bad_short_ret)
                    ev_rows.append({"ts_ms": int(r.get("ts_ms", 0) or 0), "ev_long_after_cost_pct": float(ev_long), "ev_short_after_cost_pct": float(ev_short), "confidence": conf})
                    if conf < thr or pred_lbl not in {"long_good", "short_good"}:
                        continue
                    if pred_lbl == "long_good" and ev_long <= min_ev_threshold:
                        continue
                    if pred_lbl == "short_good" and ev_short <= min_ev_threshold:
                        continue
                    move_pct = float(r.get("actual_future_return_pct", 0.0) or 0.0)
                    if pred_lbl == "long_good":
                        long_total += 1
                        ret = move_pct - cost_pct
                        if act_lbl == "long_good":
                            true_long += 1
                    else:
                        short_total += 1
                        ret = (-move_pct) - cost_pct
                        if act_lbl == "short_good":
                            true_short += 1
                    directional_samples += 1
                    pnl_sum += ret
                    rets.append(ret)
                directional_trace = [r for r in trace if str(r.get("predicted_label", "no_trade") or "no_trade").lower() in {"long_good", "short_good"}]
                directional_sorted = sorted(directional_trace, key=lambda rr: float(rr.get("confidence", 0.0) or 0.0), reverse=True)
                for frac in topk_grid:
                    k = max(1, int(len(directional_sorted) * frac))
                    rows_k = directional_sorted[:k]
                    if not rows_k:
                        continue
                    t_rets = []
                    long_hits = short_hits = long_n = short_n = 0
                    for rr in rows_k:
                        pl = str(rr.get("predicted_label", "no_trade")).lower()
                        al = str(rr.get("actual_label", "no_trade")).lower()
                        mv = float(rr.get("actual_future_return_pct", 0.0) or 0.0)
                        if pl == "long_good":
                            long_n += 1; long_hits += int(al == "long_good"); t_rets.append(mv - cost_pct)
                        elif pl == "short_good":
                            short_n += 1; short_hits += int(al == "short_good"); t_rets.append((-mv) - cost_pct)
                    topk_eval_rows.append({"top_k_pct": float(frac * 100.0), "sample_count": int(len(rows_k)), "avg_return_after_cost_pct": float(np.mean(t_rets)) if t_rets else 0.0, "long_precision": float(long_hits / max(1, long_n)), "short_precision": float(short_hits / max(1, short_n))})
                for margin in margin_grid:
                    rows_m = []
                    for rr in trace:
                        pl = str(rr.get("predicted_label", "no_trade")).lower()
                        p_long = float(rr.get("prob_up", 0.0) or 0.0); p_short = float(rr.get("prob_down", 0.0) or 0.0); p_hold = float(rr.get("prob_flat", 0.0) or 0.0)
                        if pl == "long_good" and (p_long - max(p_short, p_hold)) < margin: continue
                        if pl == "short_good" and (p_short - max(p_long, p_hold)) < margin: continue
                        rows_m.append(rr)
                    sel = [rr for rr in rows_m if str(rr.get("predicted_label", "no_trade")).lower() in {"long_good", "short_good"}]
                    mrets = [((float(rr.get("actual_future_return_pct", 0.0) or 0.0) - cost_pct) if str(rr.get("predicted_label", "")).lower() == "long_good" else ((-float(rr.get("actual_future_return_pct", 0.0) or 0.0)) - cost_pct)) for rr in sel]
                    margin_sweep_rows.append({"margin": float(margin), "sample_count": int(len(sel)), "avg_return_after_cost_pct": float(np.mean(mrets)) if mrets else 0.0})
                for lo, hi in confidence_buckets:
                    rows_b = [rr for rr in trace if float(rr.get("confidence", 0.0) or 0.0) >= lo and float(rr.get("confidence", 0.0) or 0.0) < hi and str(rr.get("predicted_label", "no_trade")).lower() in {"long_good", "short_good"}]
                    b_rets = [((float(rr.get("actual_future_return_pct", 0.0) or 0.0) - cost_pct) if str(rr.get("predicted_label", "")).lower() == "long_good" else ((-float(rr.get("actual_future_return_pct", 0.0) or 0.0)) - cost_pct)) for rr in rows_b]
                    hits = sum(1 for rr in rows_b if str(rr.get("predicted_label", "")).lower() == str(rr.get("actual_label", "")).lower())
                    bucket_eval_rows.append({"bucket": f"{lo:.2f}-{min(hi,1.0):.2f}", "sample_count": int(len(rows_b)), "precision": float(hits / max(1, len(rows_b))), "avg_return_after_cost_pct": float(np.mean(b_rets)) if b_rets else 0.0})
                random_baseline = float(np.mean(rets)) if rets else 0.0
                no_trade_baseline = 0.0
                trades = long_total + short_total
                action_rate = float(trades / max(1, len(trace)))
                avg_trade_ret = float(np.mean(rets)) if rets else 0.0
                pnl_after_cost = float(pnl_sum / max(1, trades))
                if trades <= 0:
                    final_test_avg_trade_return_after_cost = 0.0
                    final_test_pnl_after_cost = 0.0
                    final_test_reason = "no_gated_trades"
                else:
                    final_test_avg_trade_return_after_cost = float(avg_trade_ret)
                    final_test_pnl_after_cost = float(pnl_after_cost)
                    final_test_reason = "ok"
                false_long_rate = float((long_total - true_long) / max(1, long_total))
                false_short_rate = float((short_total - true_short) / max(1, short_total))
                long_precision = float(true_long / max(1, long_total))
                short_precision = float(true_short / max(1, short_total))
                long_allowed = bool(long_total >= min_directional_samples and false_long_rate <= max_false_long_rate and long_precision >= min_long_precision)
                short_allowed = bool(short_total >= min_directional_samples and false_short_rate <= max_false_short_rate and short_precision >= min_short_precision)
                # simple 5-fold chronological walk-forward on current split for stability audit
                wf_rows: list[dict[str, Any]] = []
                fold_size = max(1, len(trace) // 5)
                val_pos_test_neg = 0
                for fi in range(5):
                    v0 = fi * fold_size
                    v1 = min(len(trace), (fi + 1) * fold_size)
                    t0 = v1
                    t1 = min(len(trace), t0 + fold_size)
                    if t0 >= len(trace) or v0 >= v1:
                        continue
                    vrows = trace[v0:v1]
                    trows = trace[t0:t1]
                    if not trows:
                        continue
                    best_thr = thr
                    best_val = float("-inf")
                    for cand in [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]:
                        vpnl = 0.0
                        vtr = 0
                        for rr in vrows:
                            pl = str(rr.get("predicted_label", "no_trade") or "no_trade").lower()
                            cf = float(rr.get("confidence", 0.0) or 0.0)
                            if cf < cand or pl not in {"long_good", "short_good"}:
                                continue
                            mv = float(rr.get("actual_future_return_pct", 0.0) or 0.0)
                            vpnl += (mv - cost_pct) if pl == "long_good" else ((-mv) - cost_pct)
                            vtr += 1
                        score = float(vpnl / max(1, vtr))
                        if score > best_val:
                            best_val = score
                            best_thr = cand
                    tpnl = 0.0
                    ttr = 0
                    tlong = tshort = ttrue_l = ttrue_s = 0
                    for rr in trows:
                        pl = str(rr.get("predicted_label", "no_trade") or "no_trade").lower()
                        al = str(rr.get("actual_label", "no_trade") or "no_trade").lower()
                        cf = float(rr.get("confidence", 0.0) or 0.0)
                        if cf < best_thr or pl not in {"long_good", "short_good"}:
                            continue
                        mv = float(rr.get("actual_future_return_pct", 0.0) or 0.0)
                        tpnl += (mv - cost_pct) if pl == "long_good" else ((-mv) - cost_pct)
                        ttr += 1
                        if pl == "long_good":
                            tlong += 1
                            if al == "long_good":
                                ttrue_l += 1
                        else:
                            tshort += 1
                            if al == "short_good":
                                ttrue_s += 1
                    t_action_rate = float(ttr / max(1, len(trows)))
                    t_pnl = float(tpnl / max(1, ttr))
                    t_lp = float(ttrue_l / max(1, tlong))
                    t_sp = float(ttrue_s / max(1, tshort))
                    t_flr = float((tlong - ttrue_l) / max(1, tlong))
                    t_fsr = float((tshort - ttrue_s) / max(1, tshort))
                    fail_reasons: list[str] = []
                    if t_pnl <= 0:
                        fail_reasons.append("negative_test_pnl_after_cost")
                    if t_action_rate < min_action_rate:
                        fail_reasons.append("below_minimum_action_rate")
                    if ttr < min_directional_samples:
                        fail_reasons.append("below_minimum_directional_samples")
                    if t_lp < min_long_precision:
                        fail_reasons.append("long_precision_too_low")
                    if t_sp < min_short_precision:
                        fail_reasons.append("short_precision_too_low")
                    if t_flr > max_false_long_rate:
                        fail_reasons.append("false_long_rate_too_high")
                    if t_fsr > max_false_short_rate:
                        fail_reasons.append("false_short_rate_too_high")
                    if best_val > 0 and t_pnl < 0:
                        val_pos_test_neg += 1
                    wf_rows.append(
                        {
                            "fold_index": int(fi + 1),
                            "selected_threshold": float(best_thr),
                            "val_pnl_proxy_after_cost": float(best_val),
                            "test_pnl_proxy_after_cost": float(t_pnl),
                            "test_action_rate": float(t_action_rate),
                            "test_long_precision": float(t_lp),
                            "test_short_precision": float(t_sp),
                            "test_false_long_rate": float(t_flr),
                            "test_false_short_rate": float(t_fsr),
                            "test_directional_samples": int(ttr),
                            "pass": bool(len(fail_reasons) == 0),
                            "fail_reasons": fail_reasons,
                        }
                    )
                wf_pnls = [float(x.get("test_pnl_proxy_after_cost", 0.0) or 0.0) for x in wf_rows]
                wf_avg = float(np.mean(wf_pnls)) if wf_pnls else 0.0
                wf_worst = float(min(wf_pnls)) if wf_pnls else 0.0
                wf_pass = int(sum(1 for x in wf_rows if bool(x.get("pass", False))))
                wf_fail = int(max(0, len(wf_rows) - wf_pass))
                wf_latest_pass = bool(wf_rows[-1].get("pass", False)) if wf_rows else False
                approval_reasons: list[str] = []
                if wf_avg <= 0.0:
                    approval_reasons.append("walk_forward_avg_not_positive")
                if wf_worst < -0.0010:
                    approval_reasons.append("walk_forward_worst_below_limit")
                if wf_pass < 4:
                    approval_reasons.append("walk_forward_insufficient_pass_count")
                if not wf_latest_pass:
                    approval_reasons.append("walk_forward_latest_fold_failed")
                if val_pos_test_neg >= 2:
                    approval_reasons.append("validation_overfit_or_test_failed")
                if not (long_allowed or short_allowed):
                    approval_reasons.append("side_quality_block")
                if directional_samples < min_directional_samples:
                    approval_reasons.append("insufficient_directional_samples")
                if action_rate < min_action_rate:
                    approval_reasons.append("below_minimum_action_rate")
                approval_ok = len(approval_reasons) == 0 and pnl_after_cost > 0 and avg_trade_ret > 0
                if long_allowed and short_allowed:
                    live_mode = "LONG_SHORT_ENABLED" if approval_ok else "PAPER_ONLY"
                elif long_allowed:
                    live_mode = "LONG_ONLY" if approval_ok else "PAPER_ONLY"
                elif short_allowed:
                    live_mode = "SHORT_ONLY" if approval_ok else "PAPER_ONLY"
                else:
                    live_mode = "HOLD_ONLY"
                calibration_updates = {
                    "test_gated_confidence_threshold_used": float(thr),
                    "test_gated_action_rate": float(action_rate),
                    "test_gated_directional_samples": int(directional_samples),
                    "test_gated_pnl_proxy_after_cost": float(pnl_after_cost),
                    "test_gated_avg_trade_return_after_cost": float(avg_trade_ret),
                    "long_precision": float(long_precision),
                    "short_precision": float(short_precision),
                    "false_long_rate": float(false_long_rate),
                    "false_short_rate": float(false_short_rate),
                    "long_allowed": bool(long_allowed),
                    "short_allowed": bool(short_allowed),
                    "walk_forward_enabled": True,
                    "walk_forward_fold_count": 5,
                    "walk_forward_test_pnl_after_cost_avg": float(wf_avg),
                    "walk_forward_test_pnl_after_cost_worst": float(wf_worst),
                    "walk_forward_pass_count": int(wf_pass),
                    "walk_forward_fail_count": int(wf_fail),
                    "walk_forward_approval_ok": bool(approval_ok),
                    "walk_forward_rejection_reasons": approval_reasons,
                    "walk_forward_folds": wf_rows,
                    "test_gated_approval_ok": bool(approval_ok),
                    "model_approval_status": "APPROVABLE" if approval_ok else "REJECTED",
                    "deploy_allowed": bool(approval_ok),
                    "live_trade_allowed": bool(approval_ok and live_mode in {"LONG_ONLY", "SHORT_ONLY", "LONG_SHORT_ENABLED"}),
                    "approval_ok": bool(approval_ok),
                    "approval_reason": "ok" if approval_ok else (approval_reasons[0] if approval_reasons else "validation_overfit_or_test_failed"),
                    "recommended_live_mode": live_mode,
                    "ev_threshold_used_pct": float(min_ev_threshold),
                    "final_test_avg_trade_return_after_cost": float(final_test_avg_trade_return_after_cost),
                    "final_test_pnl_after_cost": float(final_test_pnl_after_cost),
                    "directional_samples": int(directional_samples),
                    "final_test_reason": str(final_test_reason),
                    "ev_summary": {
                        "avg_true_long_good_return_after_cost": float(avg_true_long_good_ret),
                        "avg_true_short_good_return_after_cost": float(avg_true_short_good_ret),
                        "avg_bad_long_return_after_cost": float(avg_bad_long_ret),
                        "avg_bad_short_return_after_cost": float(avg_bad_short_ret),
                    },
                    "ev_row_sample": ev_rows[:2000],
                    "probability_margin_sweep": margin_sweep_rows,
                    "top_k_selective_eval": topk_eval_rows,
                    "confidence_bucket_eval_after_cost": bucket_eval_rows,
                    "baselines_after_cost": {"random_directional_same_rate_proxy": float(random_baseline), "always_no_trade": float(no_trade_baseline)},
                    "signal_quality_failed": bool((topk_eval_rows and max(float(x.get("avg_return_after_cost_pct", 0.0) or 0.0) for x in topk_eval_rows) <= 0.0) and avg_trade_ret <= 0.0),
                }
                await self._emit_run_log(
                    run_id,
                    "INFO" if approval_ok else "WARNING",
                    "Trade-outcome final approval: "
                    f"approval_ok={approval_ok} reason={calibration_updates['approval_reason']} "
                    f"live_mode={live_mode} long_allowed={long_allowed} short_allowed={short_allowed} "
                    f"test_pnl_after_cost={pnl_after_cost:.6f} action_rate={action_rate:.6f}",
                )
                if not approval_ok:
                    await self._emit_run_log(
                        run_id,
                        "WARNING",
                        "Walk-forward rejection reasons: "
                        + (", ".join(approval_reasons) if approval_reasons else "validation_overfit_or_test_failed"),
                    )
            elif not all(k in exm for k in ("recommended_confidence_threshold", "recommended_quality_threshold", "recommended_confidence_gap")):
                calibration_updates = {
                    "calibration_sweep_ran": False,
                    "calibration_skip_reason": "validation trace not captured",
                }
        await self._merge_run_metrics(
            run_id,
            {
                "target_mode": target_mode_value,
                "gate_profile_used": gate_profile,
                "confidence_threshold_used": float(confidence_threshold),
                "quality_threshold_used": float(quality_threshold),
                "min_confidence_gap_used": float(min_confidence_gap),
                "entry_allowed_rate": float(entry_allowed / max(1, total)),
                "directional_prediction_rate": float(directional_predictions / max(1, total)),
                "directional_action_rate": float(directional_actions / max(1, total)),
                "blocked_reasons": blocked_reasons,
                "action_counts": action_counts,
                "trade_rate_by_class": trade_rate_by_class,
                "abstention_rate": abstention_rate,
                "abstention_correctness": abstention_correctness,
                "expected_edge_by_confidence_bucket": bucket_stats,
                "confidence_distribution_histogram": confidence_distribution_histogram,
                "confidence_spread_stats": confidence_spread_stats,
                "future_return_distribution_by_label": future_return_distribution_by_label,
                "feature_separability": feature_separability,
                "feature_coverage": feature_coverage,
                "source_health": source_health,
                "split_label_distribution": split_label_distribution,
                "split_stale_rates": split_stale_rates,
                "gate_paralyzed": gate_paralyzed,
                "decision_trace_hash": decision_trace_hash,
                "deterministic_replay_expected": True,
                "feature_set_version": "phase2_v1",
                "feature_columns_used": list(FEATURE_COLUMNS),
                "ablation_excluded_features": list(config.get("ablation_excluded_features", []) or []),
                **calibration_updates,
            },
            emit_update=True,
        )
        await self._repository.replace_run_prediction_trace(
            run_id,
            pair_symbol,
            trace,
            epoch_index=int(epoch_index),
            split=split_value,
        )

    async def _merge_run_metrics(self, run_id: str, updates: dict[str, Any], *, emit_update: bool = True) -> dict[str, Any]:
        run = await self._repository.get_run(run_id)
        if run is None:
            return {}
        current = run.get("metrics", {})
        merged: dict[str, Any] = dict(current) if isinstance(current, dict) else {}
        merged.update(updates)
        await self._repository.update_run(
            run_id,
            {
                "metrics": merged,
                "updated_at": utc_now_iso(),
            },
        )
        if emit_update:
            await self._emit_run_update(run_id)
        return merged

    async def _runtime_boundary_cleanup(
        self,
        *,
        run_id: str,
        reason: str,
        torch_module: Any | None = None,
        force: bool = False,
        cooldown_seconds: float = 20.0,
    ) -> None:
        now = time.time()
        last_map = getattr(self, "_cleanup_last_ts_by_run", None)
        if not isinstance(last_map, dict):
            last_map = {}
            self._cleanup_last_ts_by_run = last_map
        last_ts = float(last_map.get(run_id, 0.0) or 0.0)
        if (not force) and (now - last_ts < cooldown_seconds):
            return
        collected = 0
        try:
            collected = int(gc.collect())
        except Exception:
            collected = 0
        cuda_cleared = False
        if torch_module is not None:
            try:
                if bool(getattr(torch_module, "cuda", None)) and torch_module.cuda.is_available():
                    torch_module.cuda.empty_cache()
                    cuda_cleared = True
            except Exception:
                cuda_cleared = False
        last_map[run_id] = now
        await self._emit_run_log(
            run_id,
            "INFO",
            f"Boundary cleanup executed ({reason}): gc_collected={collected}, cuda_empty_cache={cuda_cleared}",
        )

    @staticmethod
    def _stream_checkpoint_path(run_dir: Path) -> Path:
        return run_dir / "stream_checkpoint.pt"

    @staticmethod
    def _stream_checkpoint_sidecar_path(run_dir: Path) -> Path:
        return run_dir / "stream_checkpoint.json"

    def _snapshot_rng_state(self, torch_module: Any) -> dict[str, Any]:
        state: dict[str, Any] = {
            "python_random_state": random.getstate(),
            "numpy_random_state": np.random.get_state() if np is not None else None,
        }
        try:
            state["torch_cpu_rng_state"] = torch_module.get_rng_state().cpu().tolist()
        except Exception:
            state["torch_cpu_rng_state"] = None
        try:
            if torch_module.cuda.is_available():
                state["torch_cuda_rng_state_all"] = [item.cpu().tolist() for item in torch_module.cuda.get_rng_state_all()]
            else:
                state["torch_cuda_rng_state_all"] = None
        except Exception:
            state["torch_cuda_rng_state_all"] = None
        return state

    def _restore_rng_state(self, torch_module: Any, payload: dict[str, Any]) -> None:
        py_state = payload.get("python_random_state")
        if py_state is not None:
            try:
                random.setstate(py_state)
            except Exception:
                pass
        np_state = payload.get("numpy_random_state")
        if np_state is not None and np is not None:
            try:
                np.random.set_state(np_state)
            except Exception:
                pass
        cpu_state = payload.get("torch_cpu_rng_state")
        if isinstance(cpu_state, list):
            try:
                import torch as _torch

                torch_module.set_rng_state(_torch.tensor(cpu_state, dtype=_torch.uint8))
            except Exception:
                pass
        cuda_states = payload.get("torch_cuda_rng_state_all")
        if isinstance(cuda_states, list):
            try:
                import torch as _torch

                tensors = [_torch.tensor(item, dtype=_torch.uint8) for item in cuda_states if isinstance(item, list)]
                if tensors:
                    torch_module.cuda.set_rng_state_all(tensors)
            except Exception:
                pass

    async def _save_stream_checkpoint(
        self,
        *,
        run_dir: Path,
        run_id: str,
        torch_module: Any,
        model: Any,
        optimizer: Any,
        scaler: Any,
        scheduler: Any,
        epoch_index: int,
        chunk_index: int,
        split_cursor: dict[str, int],
        progress: dict[str, Any],
    ) -> None:
        checkpoint_path = self._stream_checkpoint_path(run_dir)
        sidecar_path = self._stream_checkpoint_sidecar_path(run_dir)
        payload = {
            "epoch_index": int(epoch_index),
            "chunk_index": int(chunk_index),
            "split_cursor": dict(split_cursor or {}),
            "progress": dict(progress or {}),
            "rng_state": self._snapshot_rng_state(torch_module),
            "model_state_dict": model.state_dict(),
            "optimizer_state_dict": optimizer.state_dict() if optimizer is not None else None,
            "scheduler_state_dict": scheduler.state_dict() if scheduler is not None else None,
            "scaler_state_dict": scaler.state_dict() if scaler is not None else None,
            "saved_at": utc_now_iso(),
        }
        try:
            torch_module.save(payload, checkpoint_path)
            sidecar_path.write_text(
                json.dumps(
                    {
                        "run_id": run_id,
                        "epoch_index": int(epoch_index),
                        "chunk_index": int(chunk_index),
                        "split_cursor": dict(split_cursor or {}),
                        "progress": dict(progress or {}),
                        "saved_at": payload["saved_at"],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
        except Exception as exc:
            await self._emit_run_log(run_id, "WARNING", f"Checkpoint save skipped: {exc}")

    async def _load_stream_checkpoint(self, *, run_dir: Path, run_id: str, torch_module: Any) -> dict[str, Any] | None:
        checkpoint_path = self._stream_checkpoint_path(run_dir)
        if not checkpoint_path.exists():
            return None
        try:
            payload = torch_module.load(str(checkpoint_path), map_location="cpu")
            if not isinstance(payload, dict):
                return None
            await self._emit_run_log(
                run_id,
                "INFO",
                f"Stream checkpoint found: epoch={int(payload.get('epoch_index', 0))}, chunk={int(payload.get('chunk_index', 0))}",
            )
            return payload
        except Exception as exc:
            await self._emit_run_log(run_id, "WARNING", f"Checkpoint load failed; starting fresh ({exc})")
            return None

    async def _emit_run_update(self, run_id: str, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force:
            last = self._last_run_update_emit_monotonic.get(run_id, 0.0)
            if now - last < 0.5:
                return
        self._last_run_update_emit_monotonic[run_id] = now
        run = await self._repository.get_run(run_id)
        if run is None:
            return
        await self._hub.broadcast({"type": "ml_run_update", "run": run})

    async def _emit_run_log(self, run_id: str, level: str, message: str) -> None:
        await self._repository.append_run_log(run_id, level, message)
        level_up = str(level or "").upper()
        now = time.monotonic()
        # Keep websocket logs responsive during training while still avoiding
        # extreme burst spam. Persisted sqlite logs remain complete.
        if level_up == "INFO":
            last = self._last_info_log_emit_monotonic.get(run_id, 0.0)
            # Always allow key progress messages to stream to UI.
            msg_low = str(message or "").lower()
            is_progress = (
                "heartbeat" in msg_low
                or "epoch " in msg_low
                or "dataset ready" in msg_low
                or "feature build" in msg_low
                or "training " in msg_low
                or "planner coverage" in msg_low
            )
            if (now - last < 0.08) and not is_progress:
                return
            self._last_info_log_emit_monotonic[run_id] = now
        await self._hub.broadcast(
            {
                "type": "ml_run_log",
                "run_id": run_id,
                "ts": utc_now_iso(),
                "level": level_up,
                "message": message,
            }
        )

    async def _emit_bot_update(self, pair_symbol: str, instance_id: str = "") -> None:
        status = await (self.bot_status_by_instance(instance_id) if instance_id else self.bot_status(pair_symbol))
        await self._hub.broadcast({"type": "ml_bot_update", "payload": status})

    async def _write_pair_settings_file(
        self,
        pair_symbol: str,
        payload: dict[str, Any],
        *,
        instance_name: str = "Default",
        instance_id: str = "",
    ) -> None:
        instance_root = self._instance_root(pair_symbol, instance_name, instance_id)
        instance_root.mkdir(parents=True, exist_ok=True)
        settings_path = instance_root / "pair_settings.txt"
        lines = [
            f"Pair: {pair_symbol.upper()}",
            f"Instance Name: {instance_name}",
            f"Instance ID: {instance_id or '-'}",
            f"Updated: {payload['updated_at']}",
            "",
            f"Training Exchanges: {', '.join(self._training_exchanges(payload)) or '-'}",
            f"Bot Data Exchanges: {', '.join(self._bot_data_exchanges(payload)) or '-'}",
            f"Bot Execution Exchanges: {', '.join(self._bot_execution_exchanges(payload)) or '-'}",
            f"Use Local Data: {bool(payload.get('use_local_data', True))}",
            f"Use Historic Data: {bool(payload.get('use_historic_data', False))}",
            f"Historic Data Days: {int(payload.get('historic_data_days', 90))}",
            f"Horizons: {payload['horizons']}",
            f"Label Threshold (%): {payload['label_threshold_pct']}",
            f"Min Data Hours: {payload['min_data_hours']}",
            f"Normalizer Window: {payload['normalizer_window']}",
            "",
            "Training Settings:",
            json.dumps(payload["training"], indent=2),
            "",
            "Paper Bot Settings:",
            json.dumps(payload["paper_bot"], indent=2),
        ]
        settings_path.write_text("\n".join(lines), encoding="utf-8")

    async def _write_run_info_file(
        self,
        run_dir: Path,
        run_id: str,
        pair_symbol: str,
        config: dict[str, Any],
        metrics: dict[str, Any],
        device: str,
    ) -> None:
        run_info = [
            f"Run ID: {run_id}",
            f"Pair: {pair_symbol}",
            f"Completed At: {utc_now_iso()}",
            f"Device: {device}",
            f"Training Exchanges: {', '.join(self._training_exchanges(config)) or '-'}",
            f"Bot Data Exchanges: {', '.join(self._bot_data_exchanges(config)) or '-'}",
            f"Bot Execution Exchanges: {', '.join(self._bot_execution_exchanges(config)) or '-'}",
            "",
            "Config:",
            json.dumps(config, indent=2),
            "",
            "Metrics:",
            json.dumps(metrics, indent=2),
        ]
        (run_dir / "run_info.txt").write_text("\n".join(run_info), encoding="utf-8")

    def _sanitize_name(self, value: str) -> str:
        cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in (value or "").strip())
        cleaned = cleaned.strip("_")
        return cleaned[:64] or "Bot"

    def _instance_root(self, pair_symbol: str, instance_name: str, instance_id: str) -> Path:
        pair_root = self._models_root / pair_symbol.upper()
        suffix = f"__{instance_id}" if instance_id else "__default"
        folder_name = f"{self._sanitize_name(instance_name)}{suffix}"
        return pair_root / folder_name

    def _instance_source_path(self, pair_symbol: str, instance_name: str, instance_id: str) -> str:
        return str(self._instance_root(pair_symbol, instance_name, instance_id))

    async def _rename_instance_folder_prefix(self, pair_symbol: str, old_name: str, new_name: str, instance_id: str) -> None:
        old_root = self._instance_root(pair_symbol, old_name, instance_id)
        new_root = self._instance_root(pair_symbol, new_name, instance_id)
        if not old_root.exists() or old_root == new_root:
            return
        new_root.parent.mkdir(parents=True, exist_ok=True)
        try:
            old_root.rename(new_root)
        except Exception as exc:
            logger.warning("Could not rename instance folder %s -> %s: %s", old_root, new_root, exc)

    def _historic_data_root(self, pair_symbol: str, instance_name: str = "Default", instance_id: str = "") -> Path:
        return self._instance_root(pair_symbol, instance_name, instance_id) / "historic_data_acquired"

    async def _historic_data_status(
        self,
        pair_symbol: str,
        instance_name: str = "Default",
        instance_id: str = "",
    ) -> dict[str, Any]:
        root = self._historic_data_root(pair_symbol, instance_name, instance_id)
        report_path = root / "coverage_report.json"
        if report_path.exists():
            try:
                payload = json.loads(report_path.read_text(encoding="utf-8"))
                return {
                    "pair_symbol": pair_symbol.upper(),
                    "file_count": int(payload.get("file_count", 0) or 0),
                    "rows_total": int(payload.get("total_rows", 0) or 0),
                    "min_ts_ms": int(payload.get("min_ts_ms")) if payload.get("min_ts_ms") is not None else None,
                    "max_ts_ms": int(payload.get("max_ts_ms")) if payload.get("max_ts_ms") is not None else None,
                    "data_hours": float(payload.get("data_hours", 0.0) or 0.0),
                    "source_path": str(root),
                    "mode": str(payload.get("mode") or "exchange_api_hybrid"),
                    "selected_exchanges": [str(item).lower() for item in payload.get("selected_exchanges", []) if str(item).strip()],
                    "exchange_progress": [item for item in payload.get("exchange_progress", []) if isinstance(item, dict)],
                }
            except Exception:
                pass
        files = self._source_feature_files_from_root(root)
        min_ts: int | None = None
        max_ts: int | None = None
        rows_total = 0
        for file_path in files:
            rows = self._read_feature_records(file_path)
            if not rows:
                continue
            ts_values = [int(item.get("ts_ms", 0)) for item in rows if int(item.get("ts_ms", 0)) > 0]
            rows_total += len(rows)
            if not ts_values:
                continue
            current_min = min(ts_values)
            current_max = max(ts_values)
            min_ts = current_min if min_ts is None else min(min_ts, current_min)
            max_ts = current_max if max_ts is None else max(max_ts, current_max)
        data_hours = 0.0
        if min_ts is not None and max_ts is not None and max_ts > min_ts:
            data_hours = (max_ts - min_ts) / 3_600_000
        return {
            "pair_symbol": pair_symbol.upper(),
            "file_count": len(files),
            "rows_total": rows_total,
            "min_ts_ms": min_ts,
            "max_ts_ms": max_ts,
            "data_hours": data_hours,
            "source_path": str(root),
            "mode": "exchange_api_hybrid",
            "selected_exchanges": [],
            "exchange_progress": [],
        }

    def _source_feature_files_from_root(self, root: Path) -> list[Path]:
        if not root.exists():
            return []
        files: list[Path] = []
        for date_dir in sorted(root.glob("date=*")):
            if not date_dir.is_dir():
                continue
            for file_path in sorted(date_dir.iterdir()):
                if file_path.suffix.lower() in {".parquet", ".jsonl"}:
                    files.append(file_path)
        return files

    def _read_feature_records(self, file_path: Path) -> list[dict[str, Any]]:
        if not file_path.exists():
            return []
        if file_path.suffix.lower() == ".parquet":
            if pq is None:
                return []
            try:
                table = pq.read_table(file_path)
                rows = table.to_pylist()
                return [dict(item) for item in rows if isinstance(item, dict)]
            except Exception:
                return []
        if file_path.suffix.lower() == ".jsonl":
            records: list[dict[str, Any]] = []
            try:
                with file_path.open("r", encoding="utf-8") as handle:
                    for line in handle:
                        raw = line.strip()
                        if not raw:
                            continue
                        parsed = json.loads(raw)
                        if isinstance(parsed, dict):
                            records.append(parsed)
            except Exception:
                return []
            return records
        return []

    async def _default_selected_exchanges(self, pair_symbol: str) -> list[str]:
        snapshots = await self._tab_sessions.collect_ml_snapshots()
        for item in snapshots:
            if str(item.get("pair_symbol", "")).upper() == pair_symbol.upper():
                exchanges = [str(exchange).lower() for exchange in item.get("enabled_exchange_ids", [])]
                if exchanges:
                    return sorted(set(exchanges))
        return ["binance"]

    def _training_exchanges(self, payload: dict[str, Any]) -> list[str]:
        items = payload.get("training_selected_exchanges") or payload.get("selected_exchanges") or []
        return sorted({str(item).lower() for item in items if str(item).strip()})

    def _bot_data_exchanges(self, payload: dict[str, Any]) -> list[str]:
        items = payload.get("bot_data_selected_exchanges") or payload.get("selected_exchanges") or []
        return sorted({str(item).lower() for item in items if str(item).strip()})

    def _bot_execution_exchanges(self, payload: dict[str, Any]) -> list[str]:
        items = payload.get("bot_execution_selected_exchanges") or payload.get("selected_exchanges") or []
        return sorted({str(item).lower() for item in items if str(item).strip()})

    def _run_dir(self, pair_symbol: str, run_id: str, *, instance_name: str = "Default", instance_id: str = "") -> Path:
        return self._instance_root(pair_symbol, instance_name, instance_id) / "runs" / run_id

    def _profile_to_dict(self, profile: MlProfile) -> dict[str, Any]:
        return {
            "pair_symbol": profile.pair_symbol,
            "selected_exchanges": list(profile.selected_exchanges),
            "training_selected_exchanges": list(profile.training_selected_exchanges),
            "bot_data_selected_exchanges": list(profile.bot_data_selected_exchanges),
            "bot_execution_selected_exchanges": list(profile.bot_execution_selected_exchanges),
            "bot_signal_mode": str(profile.bot_signal_mode),
            "execution_position_mode": str(profile.execution_position_mode),
            "use_local_data": bool(profile.use_local_data),
            "use_historic_data": bool(profile.use_historic_data),
            "historic_data_days": int(profile.historic_data_days),
            "historic_data_days_max": int(self._historic_data_days_max),
            "horizons": list(profile.horizons),
            "label_threshold_pct": profile.label_threshold_pct,
            "min_data_hours": profile.min_data_hours,
            "normalizer_window": profile.normalizer_window,
            "replay_candle_limit": int(profile.replay_candle_limit),
            "training_hour_window_enabled": bool(profile.training_hour_window_enabled),
            "training_hour_start": max(0, int(profile.training_hour_start)),
            "training_hour_end": max(0, int(profile.training_hour_end)),
            "full_data_mode": bool(profile.full_data_mode),
            "strict_full_windows_mode": bool(profile.strict_full_windows_mode),
            "disabled_training_features": list(getattr(profile, "disabled_training_features", []) or []),
            "label_guard_enabled": bool(getattr(profile, "label_guard_enabled", True)),
            "target_mode": str(getattr(profile, "target_mode", "triple_barrier") or "triple_barrier"),
            "triple_barrier": dict(
                getattr(
                    profile,
                    "triple_barrier",
                    {"tp_pct": 0.08, "sl_pct": 0.05, "timeout_steps": 180},
                )
                or {"tp_pct": 0.08, "sl_pct": 0.05, "timeout_steps": 180}
            ),
            "training": dict(profile.training),
            "paper_bot": dict(profile.paper_bot),
            "updated_at": profile.updated_at,
        }

    async def _bot_loop(self, pair_symbol: str, stop_event: asyncio.Event, instance_id: str = "") -> None:
        pair_symbol = pair_symbol.upper()
        profile = await (self.get_profile_by_instance(instance_id) if instance_id else self.get_profile(pair_symbol))
        current_model = await self._repository.current_model(pair_symbol, instance_id)
        bot_data_exchanges = self._bot_data_exchanges(profile)
        bot_execution_exchanges = self._bot_execution_exchanges(profile)
        bot_signal_mode = str(profile.get("bot_signal_mode", "combined") or "combined").strip().lower()
        execution_position_mode = str(profile.get("execution_position_mode", "combined_position") or "combined_position").strip().lower()
        if not bot_data_exchanges:
            raise ValueError("No bot data exchanges selected for this instance.")
        if not bot_execution_exchanges:
            raise ValueError("No bot execution exchanges selected for this instance.")
        await self._tab_sessions.acquire_bot_session(
            instance_id,
            pair_symbol,
            bot_data_exchanges,
            poll_delay_ms=int(float(profile.get("paper_bot", {}).get("poll_delay_ms", 2000) or 2000)),
        )
        logger.info("Bot data session acquired: instance=%s pair=%s", instance_id, pair_symbol)
        if np is None:
            await self._repository.upsert_bot_state(
                pair_symbol,
                {
                    "status": "BLOCKED_NO_NUMPY",
                    "position_side": "",
                    "entry_price": 0.0,
                    "qty": 0.0,
                    "unrealized_pnl": 0.0,
                    "realized_pnl": 0.0,
                    "last_signal": {"action": "HOLD", "reason": "NumPy missing"},
                },
                instance_id=instance_id,
            )
            await self._emit_bot_update(pair_symbol, instance_id=instance_id)
            await self._tab_sessions.release_bot_session(instance_id)
            logger.info("Bot data session released: instance=%s pair=%s", instance_id, pair_symbol)
            return
        if current_model is None:
            await self._repository.upsert_bot_state(
                pair_symbol,
                {
                    "status": "BLOCKED_NO_MODEL",
                    "position_side": "",
                    "entry_price": 0.0,
                    "qty": 0.0,
                    "unrealized_pnl": 0.0,
                    "realized_pnl": 0.0,
                    "last_signal": {"action": "HOLD", "reason": "No approved model"},
                },
                instance_id=instance_id,
            )
            await self._emit_bot_update(pair_symbol, instance_id=instance_id)
            await self._tab_sessions.release_bot_session(instance_id)
            logger.info("Bot data session released: instance=%s pair=%s", instance_id, pair_symbol)
            return
        current_metrics = dict(current_model.get("metrics", {}) or {})
        current_target_mode = str(
            current_metrics.get("target_mode")
            or (profile.get("target_mode", "triple_barrier") if isinstance(profile, dict) else "triple_barrier")
            or "triple_barrier"
        ).strip().lower()
        model_approval_status = str(current_metrics.get("model_approval_status", "APPROVABLE") or "APPROVABLE").strip().upper()
        deploy_allowed = bool(current_metrics.get("deploy_allowed", True))
        live_trade_allowed = bool(current_metrics.get("live_trade_allowed", True))
        runtime_long_allowed = bool(current_metrics.get("long_allowed", True))
        runtime_short_allowed = bool(current_metrics.get("short_allowed", True))
        runtime_live_mode = str(current_metrics.get("recommended_live_mode", "") or "").strip().upper()
        enforce_trade_outcome_runtime_gate = current_target_mode == "trade_outcome"
        try:
            import torch
            from app.services.ml.model_arch import LSTMAttentionModel, SmallTCNModel, SmallTransformerEncoderModel
        except Exception:
            await self._repository.upsert_bot_state(
                pair_symbol,
                {
                    "status": "BLOCKED_NO_TORCH",
                    "position_side": "",
                    "entry_price": 0.0,
                    "qty": 0.0,
                    "unrealized_pnl": 0.0,
                    "realized_pnl": 0.0,
                    "last_signal": {"action": "HOLD", "reason": "PyTorch missing"},
                },
                instance_id=instance_id,
            )
            await self._emit_bot_update(pair_symbol, instance_id=instance_id)
            await self._tab_sessions.release_bot_session(instance_id)
            logger.info("Bot data session released: instance=%s pair=%s", instance_id, pair_symbol)
            return
        bundle = torch.load(current_model["model_path"], map_location="cpu")
        model_cfg = bundle["model"]
        inference_temperature = float(bundle.get("temperature_value", 1.0) or 1.0)
        lookback_steps = int(bundle.get("lookback_steps", profile["training"]["lookback_steps"]))
        model_type = str(model_cfg.get("model_type", "lstm_attention") or "lstm_attention").strip().lower()
        if model_type == "tcn":
            model = SmallTCNModel(
                input_size=len(FEATURE_COLUMNS),
                hidden_channels=int(model_cfg["hidden_size"]),
                dropout=float(model_cfg["dropout"]),
            )
        elif model_type == "transformer_encoder":
            model = SmallTransformerEncoderModel(
                input_size=len(FEATURE_COLUMNS),
                hidden_size=int(model_cfg["hidden_size"]),
                num_layers=2,
                num_heads=4,
                ff_size=128,
                dropout=float(model_cfg["dropout"]),
                max_len=max(128, lookback_steps + 8),
            )
        else:
            model = LSTMAttentionModel(
                input_size=len(FEATURE_COLUMNS),
                hidden_size=int(model_cfg["hidden_size"]),
                num_layers=int(model_cfg["num_layers"]),
                dropout=float(model_cfg["dropout"]),
            )
        model.load_state_dict(bundle["state_dict"])
        model.eval()
        max_raw_len = max(int(profile["normalizer_window"]), lookback_steps)
        raw_buffers: dict[str, deque[np.ndarray]] = {}
        seq_buffers: dict[str, deque[np.ndarray]] = {}
        leverage = max(1.0, float(profile["paper_bot"].get("leverage", 1.0)))
        commission_pct = max(0.0, float(profile["paper_bot"].get("commission_fee_pct", 0.0)))
        commission_rate = commission_pct / 100.0
        one_trade_at_time = self._as_bool(profile["paper_bot"].get("one_trade_at_time", True), True)
        initial_balance = max(1.0, float(profile["paper_bot"].get("initial_balance", 100.0)))
        take_profit_pct = max(0.0, float(profile["paper_bot"].get("take_profit_pct", 0.25)))
        stop_loss_pct = max(0.0, float(profile["paper_bot"].get("stop_loss_pct", 0.2)))
        use_trailing_stop = self._as_bool(profile["paper_bot"].get("use_trailing_stop", False), False)
        trailing_stop_pct = max(0.0, float(profile["paper_bot"].get("trailing_stop_pct", 0.15)))
        regime_filter_enabled = self._as_bool(profile["paper_bot"].get("regime_filter_enabled", True), True)
        max_spread_pct = max(0.0, float(profile["paper_bot"].get("max_spread_pct", 0.15)))
        min_trade_rate_10s = max(0.0, float(profile["paper_bot"].get("min_trade_rate_10s", 3.0)))
        min_confidence_gap = max(0.0, float(profile["paper_bot"].get("min_confidence_gap", 0.06)))
        position = _PaperPosition()
        positions_by_exchange: dict[str, _PaperPosition] = {}
        realized_pnl = 0.0
        realized_pnl_by_exchange: dict[str, float] = {"combined": 0.0}
        default_session = await self._repository.default_session(instance_id)
        active_session_id = await self._repository.active_session_id(instance_id)
        if not active_session_id:
            active_session_id = default_session.session_id
            await self._repository.set_active_session(instance_id, active_session_id)
        await self._repository.upsert_bot_state(
            pair_symbol,
            {
                "status": "RUNNING",
                "position_side": "",
                "active_session_id": active_session_id,
                "entry_price": 0.0,
                "qty": 0.0,
                "unrealized_pnl": 0.0,
                "realized_pnl": 0.0,
                "last_signal": {"action": "HOLD", "reason": "Warmup"},
            },
            instance_id=instance_id,
        )
        await self._emit_bot_update(pair_symbol, instance_id=instance_id)
        snapshot_live_logged = False
        no_snapshot_since = 0.0
        last_no_snapshot_emit = 0.0
        loop_seq = 0
        trade_open_timestamps: deque[float] = deque(maxlen=512)
        consecutive_entries = 0
        last_entry_ts = 0.0

        try:
            while not stop_event.is_set():
                loop_seq += 1
                if self._bot_pause_flags.get(instance_id, False):
                    paused_state = await self._repository.bot_state(pair_symbol, instance_id=instance_id) or {}
                    paused_state["status"] = "PAUSED"
                    await self._repository.upsert_bot_state(pair_symbol, paused_state, instance_id=instance_id)
                    await self._emit_bot_update(pair_symbol, instance_id=instance_id)
                    while self._bot_pause_flags.get(instance_id, False) and not stop_event.is_set():
                        await asyncio.sleep(0.5)
                    if stop_event.is_set():
                        break
                snapshot = await self._tab_sessions.bot_pair_snapshot(instance_id)
                signal_snapshots = await self._tab_sessions.bot_pair_snapshots_by_exchange(instance_id, bot_data_exchanges)
                if snapshot is None and not signal_snapshots:
                    now_mono = time.monotonic()
                    if no_snapshot_since <= 0.0:
                        no_snapshot_since = now_mono
                    no_snapshot_sec = max(0.0, now_mono - no_snapshot_since)
                    if now_mono - last_no_snapshot_emit >= 1.0:
                        stale_signal = {
                            "action": "HOLD",
                            "confidence": 0.0,
                            "confidence_gap": 0.0,
                            "quality": 0.0,
                            "magnitude_pct": 0.0,
                            "reason": "no_snapshot_data",
                            "data_source": "bot_session",
                            "snapshot_freshness_sec": round(no_snapshot_sec, 3),
                            "active_exchange_count": 0,
                            "bot_signal_mode": bot_signal_mode,
                            "execution_position_mode": execution_position_mode,
                            "bot_data_exchanges": list(bot_data_exchanges),
                            "execution_exchanges": list(bot_execution_exchanges),
                            "loop_seq": loop_seq,
                            "bot_tick_ms": int(time.time() * 1000),
                        }
                        await self._repository.upsert_bot_state(
                            pair_symbol,
                            {
                                "status": "RUNNING",
                                "position_side": position.side if execution_position_mode != "separate_positions" else "",
                                "active_session_id": active_session_id,
                                "entry_price": position.entry_price if execution_position_mode != "separate_positions" else 0.0,
                                "qty": position.qty if execution_position_mode != "separate_positions" else 0.0,
                                "unrealized_pnl": 0.0,
                                "realized_pnl": float(sum(realized_pnl_by_exchange.values())),
                                "last_signal": stale_signal,
                            },
                            instance_id=instance_id,
                        )
                        await self._emit_bot_update(pair_symbol, instance_id=instance_id)
                        last_no_snapshot_emit = now_mono
                    await asyncio.sleep(1.0)
                    continue
                no_snapshot_since = 0.0
                if not snapshot_live_logged:
                    logger.info("Bot data session snapshot active: instance=%s pair=%s", instance_id, pair_symbol)
                    snapshot_live_logged = True

                def _infer(source_key: str, snap: dict[str, Any]) -> dict[str, Any]:
                    raw_buf = raw_buffers.setdefault(source_key, deque(maxlen=max_raw_len))
                    seq_buf = seq_buffers.setdefault(source_key, deque(maxlen=lookback_steps))
                    vec = self._vector_from_snapshot(snap)
                    raw_buf.append(vec)
                    seq_buf.append(self._normalize_vector(vec, raw_buf))
                    if len(seq_buf) < lookback_steps:
                        return {"action": "HOLD", "confidence": 0.0, "confidence_gap": 0.0, "quality": 0.0, "magnitude_pct": 0.0, "reason": "Warmup"}
                    x = torch.tensor(np.stack(seq_buf)[None, ...], dtype=torch.float32)
                    with torch.no_grad():
                        dir_logits, mag_out, quality_logits = model(x)
                        probs = torch.softmax(dir_logits / max(1e-6, inference_temperature), dim=1).cpu().numpy()[0]
                        idx = int(np.argmax(probs))
                        conf = float(probs[idx])
                        sorted_probs = sorted((float(v) for v in probs), reverse=True)
                        gap = float(sorted_probs[0] - sorted_probs[1]) if len(sorted_probs) > 1 else 1.0
                        entropy = float(-(np.asarray(probs, dtype=np.float64) * np.log(np.maximum(np.asarray(probs, dtype=np.float64), 1e-12))).sum())
                        qual = float(torch.sigmoid(quality_logits).cpu().numpy()[0])
                        # Model magnitude head outputs fractional return; expose bot signal as percentage.
                        mag = float(mag_out.cpu().numpy()[0]) * 100.0
                        gate_mode = str(profile["paper_bot"].get("gate_mode", "confidence_only") or "confidence_only").strip().lower()
                        confidence_threshold = float(profile["paper_bot"].get("confidence_threshold", 0.72) or 0.72)
                        quality_threshold = float(profile["paper_bot"].get("quality_threshold", 0.72) or 0.72)
                        min_gap = max(0.0, float(profile["paper_bot"].get("min_confidence_gap", 0.0) or 0.0))
                        gate_profile_local = str(profile["paper_bot"].get("gate_profile", "balanced") or "balanced").strip().lower()
                        entry_allowed, action, gate_flag = self._evaluate_decision(
                            pred_idx=idx,
                            conf=conf,
                            qual=qual,
                            gap=gap,
                            gate_mode=gate_mode,
                            confidence_threshold=confidence_threshold,
                            quality_threshold=quality_threshold,
                            min_confidence_gap=min_gap,
                            gate_profile=gate_profile_local,
                        )
                        reason = "Model inference" if entry_allowed else str(gate_flag)
                        return {
                            "action": action,
                            "confidence": conf,
                            "confidence_gap": gap,
                            "prob_gap": gap,
                            "entropy": entropy,
                            "quality": qual,
                            "magnitude_pct": mag,
                            "reason": reason,
                            "gate_mode": gate_mode,
                        }

                candidates: dict[str, dict[str, Any]] = {}
                if bot_signal_mode == "combined" and snapshot is not None:
                    candidates["combined"] = _infer("combined", snapshot)
                elif signal_snapshots:
                    for ex_id, snap in signal_snapshots.items():
                        candidates[ex_id] = _infer(ex_id, snap)
                elif snapshot is not None:
                    candidates["fallback"] = _infer("fallback", snapshot)

                signal = {"action": "HOLD", "confidence": 0.0, "quality": 0.0, "reason": "Warmup"}
                if candidates:
                    if bot_signal_mode == "best" and len(candidates) > 1:
                        ex_id, best_sig = max(candidates.items(), key=lambda item: float(item[1].get("confidence", 0.0) or 0.0))
                        signal = dict(best_sig)
                        signal["signal_exchange_id"] = ex_id
                    elif bot_signal_mode == "majority" and len(candidates) > 1:
                        votes = {"LONG": 0, "SHORT": 0, "HOLD": 0}
                        for sig in candidates.values():
                            votes[str(sig.get("action", "HOLD"))] = votes.get(str(sig.get("action", "HOLD")), 0) + 1
                        if votes["LONG"] > votes["SHORT"] and votes["LONG"] > votes["HOLD"]:
                            action = "LONG"
                        elif votes["SHORT"] > votes["LONG"] and votes["SHORT"] > votes["HOLD"]:
                            action = "SHORT"
                        else:
                            action = "HOLD"
                        pool = [s for s in candidates.values() if str(s.get("action")) == action] or list(candidates.values())
                        signal = dict(max(pool, key=lambda s: float(s.get("confidence", 0.0) or 0.0)))
                        signal["action"] = action
                        signal["vote_tally"] = votes
                    else:
                        signal = dict(next(iter(candidates.values())))
                if enforce_trade_outcome_runtime_gate:
                    if model_approval_status == "REJECTED" or not deploy_allowed or not live_trade_allowed:
                        signal["action"] = "HOLD"
                        signal["reason"] = "model_rejected"
                    else:
                        if runtime_live_mode == "LONG_ONLY" and signal.get("action") == "SHORT":
                            signal["action"] = "HOLD"
                            signal["reason"] = "short_side_quality_block"
                        elif runtime_live_mode == "SHORT_ONLY" and signal.get("action") == "LONG":
                            signal["action"] = "HOLD"
                            signal["reason"] = "long_side_quality_block"
                        elif runtime_live_mode in {"HOLD_ONLY", "PAPER_ONLY"} and signal.get("action") in {"LONG", "SHORT"}:
                            signal["action"] = "HOLD"
                            signal["reason"] = "model_rejected"
                        elif signal.get("action") == "LONG" and not runtime_long_allowed:
                            signal["action"] = "HOLD"
                            signal["reason"] = "long_side_quality_block"
                        elif signal.get("action") == "SHORT" and not runtime_short_allowed:
                            signal["action"] = "HOLD"
                            signal["reason"] = "short_side_quality_block"

                base_snapshot = snapshot or next(iter(signal_snapshots.values()), {})
                snapshot_ts_ms = int(base_snapshot.get("ts_ms", 0) or 0)
                now_ts_ms = int(time.time() * 1000)
                freshness_sec = 0.0
                if snapshot_ts_ms > 0:
                    freshness_sec = max(0.0, (now_ts_ms - snapshot_ts_ms) / 1000.0)
                max_signal_age_ms = max(0, int(profile["paper_bot"].get("max_signal_age_ms", 2000) or 2000))
                if signal.get("action") in {"LONG", "SHORT"} and snapshot_ts_ms > 0 and (now_ts_ms - snapshot_ts_ms) > max_signal_age_ms:
                    signal["action"] = "HOLD"
                    signal["reason"] = "stale_signal_block"
                mid_price = float(base_snapshot.get("mid_price", 0.0) or 0.0)
                spread = abs(float(base_snapshot.get("spread", 0.0) or 0.0))
                spread_pct = (spread / mid_price * 100.0) if mid_price > 0 else 0.0
                trade_rate_10s = float(base_snapshot.get("trade_rate_10s", 0.0) or 0.0)
                max_spread_bps = max(0.0, float(profile["paper_bot"].get("max_spread_bps", 15.0) or 15.0))
                min_orderbook_depth = max(0.0, float(profile["paper_bot"].get("min_orderbook_depth", 0.0) or 0.0))
                max_trades_per_hour = max(0, int(profile["paper_bot"].get("max_trades_per_hour", 30) or 30))
                max_consecutive_entries = max(0, int(profile["paper_bot"].get("max_consecutive_entries", 5) or 5))
                min_seconds_between_entries = max(0, int(profile["paper_bot"].get("min_seconds_between_entries", 5) or 5))
                spread_bps = (spread / mid_price * 10000.0) if mid_price > 0 else 0.0
                book_depth = float(base_snapshot.get("top10_bid_volume", 0.0) or 0.0) + float(base_snapshot.get("top10_ask_volume", 0.0) or 0.0)
                if signal["action"] in {"LONG", "SHORT"} and regime_filter_enabled:
                    if spread_pct > max_spread_pct:
                        signal["action"] = "HOLD"; signal["reason"] = "regime_block_spread"
                    elif trade_rate_10s < min_trade_rate_10s:
                        signal["action"] = "HOLD"; signal["reason"] = "regime_block_low_trade_speed"
                    elif float(signal.get("confidence_gap", 0.0) or 0.0) < min_confidence_gap:
                        signal["action"] = "HOLD"; signal["reason"] = "regime_block_low_confidence_gap"
                if signal["action"] in {"LONG", "SHORT"}:
                    now_sec = time.time()
                    while trade_open_timestamps and (now_sec - trade_open_timestamps[0]) > 3600.0:
                        trade_open_timestamps.popleft()
                    if max_spread_bps > 0.0 and spread_bps > max_spread_bps:
                        signal["action"] = "HOLD"; signal["reason"] = "execution_block_spread_bps"
                    elif min_orderbook_depth > 0.0 and book_depth < min_orderbook_depth:
                        signal["action"] = "HOLD"; signal["reason"] = "execution_block_low_depth"
                    elif max_trades_per_hour > 0 and len(trade_open_timestamps) >= max_trades_per_hour:
                        signal["action"] = "HOLD"; signal["reason"] = "frequency_block_max_trades_per_hour"
                    elif max_consecutive_entries > 0 and consecutive_entries >= max_consecutive_entries:
                        signal["action"] = "HOLD"; signal["reason"] = "frequency_block_max_consecutive_entries"
                    elif min_seconds_between_entries > 0 and (now_sec - last_entry_ts) < min_seconds_between_entries:
                        signal["action"] = "HOLD"; signal["reason"] = "frequency_block_min_seconds_between_entries"
                # Live calibration safety guard (no auto-threshold mutation).
                live_guard_action = str(profile["paper_bot"].get("live_calibration_guard_action", "monitor_only") or "monitor_only").strip().lower()
                live_ece_score = abs(float(signal.get("confidence", 0.0) or 0.0) - float(signal.get("quality", 0.0) or 0.0))
                live_brier_score = float((1.0 - float(signal.get("confidence", 0.0) or 0.0)) ** 2)
                signal["live_ece_score"] = round(live_ece_score, 6)
                signal["live_brier_score"] = round(live_brier_score, 6)
                if signal["action"] in {"LONG", "SHORT"} and live_guard_action == "block_entries":
                    ece_limit = 0.25
                    brier_limit = 0.35
                    if live_ece_score > ece_limit or live_brier_score > brier_limit:
                        signal["action"] = "HOLD"
                        signal["reason"] = "calibration_drift_block"

                exec_snapshots = await self._tab_sessions.bot_pair_snapshots_by_exchange(instance_id, bot_execution_exchanges)

                if execution_position_mode == "separate_positions":
                    for ex_id in bot_execution_exchanges:
                        positions_by_exchange.setdefault(ex_id, _PaperPosition())
                        realized_pnl_by_exchange.setdefault(ex_id, 0.0)
                else:
                    realized_pnl_by_exchange.setdefault("combined", 0.0)

                def _entry_context_for(signal_obj: dict[str, Any], snap_obj: dict[str, Any], entry_ex_id: str) -> dict[str, Any]:
                    return {
                        "entry_reason": str(signal_obj.get("reason", "model_inference") or "model_inference"),
                        "entry_confidence": float(signal_obj.get("confidence", 0.0) or 0.0),
                        "entry_quality": float(signal_obj.get("quality", 0.0) or 0.0),
                        "entry_confidence_gap": float(signal_obj.get("confidence_gap", 0.0) or 0.0),
                        "entry_spread_pct": float(
                            ((abs(float(snap_obj.get("spread", 0.0) or 0.0)) / max(float(snap_obj.get("mid_price", 0.0) or 0.0), 1e-12)) * 100.0)
                            if float(snap_obj.get("mid_price", 0.0) or 0.0) > 0
                            else 0.0
                        ),
                        "entry_trade_rate_10s": float(snap_obj.get("trade_rate_10s", 0.0) or 0.0),
                        "entry_ladder_buy_pct": float(snap_obj.get("ladder_buy_pct", 0.0) or 0.0),
                        "entry_ladder_sell_pct": float(snap_obj.get("ladder_sell_pct", 0.0) or 0.0),
                        "entry_liq_events_60s": float(snap_obj.get("liq_events_60s", 0.0) or 0.0),
                        "entry_execution_exchange_id": str(entry_ex_id or "unknown"),
                        "bot_signal_mode": bot_signal_mode,
                        "execution_position_mode": execution_position_mode,
                    }

                def _close_pnl(pos: _PaperPosition, px: float) -> tuple[float, float, float, float, float]:
                    gross = (px - pos.entry_price) * pos.qty * leverage if pos.side == "LONG" else (pos.entry_price - px) * pos.qty * leverage
                    en = pos.entry_price * pos.qty * leverage
                    ex = px * pos.qty * leverage
                    ef = en * commission_rate
                    xf = ex * commission_rate
                    return gross, en, ex, ef, xf

                if execution_position_mode == "separate_positions":
                    for ex_id, pos in positions_by_exchange.items():
                        snap = exec_snapshots.get(ex_id) or base_snapshot
                        px = float(snap.get("mid_price", 0.0) or 0.0)
                        if px <= 0:
                            continue
                        if pos.side == "" and signal["action"] in {"LONG", "SHORT"}:
                            if one_trade_at_time and any(p.side for p in positions_by_exchange.values()):
                                continue
                            pos.side = signal["action"]; pos.entry_price = px; pos.qty = float(profile["paper_bot"]["max_position_qty"]); pos.opened_at = int(time.time()); pos.peak_price = px; pos.trough_price = px
                            pos.entry_context = _entry_context_for(signal, snap, ex_id)
                            trade_open_timestamps.append(time.time())
                            last_entry_ts = time.time()
                            consecutive_entries += 1
                        elif pos.side:
                            pnl_pct = ((px - pos.entry_price) / max(pos.entry_price, 1e-12)) * 100.0
                            if pos.side == "SHORT":
                                pnl_pct = -pnl_pct
                            if pos.side == "LONG":
                                pos.peak_price = max(pos.peak_price, px)
                            else:
                                pos.trough_price = min(pos.trough_price, px)
                            close_reason = ""
                            if take_profit_pct > 0 and pnl_pct >= take_profit_pct:
                                close_reason = "take_profit_hit"
                            elif stop_loss_pct > 0 and pnl_pct <= -stop_loss_pct:
                                close_reason = "stop_loss_hit"
                            elif signal["action"] in {"LONG", "SHORT"} and signal["action"] != pos.side:
                                close_reason = "opposite_signal"
                            elif int(time.time()) - pos.opened_at > int(profile["paper_bot"]["max_hold_seconds"]):
                                close_reason = "max_hold_reached"
                            should_close = bool(close_reason)
                            if should_close:
                                gross, en, ex, ef, xf = _close_pnl(pos, px)
                                net = gross - ef - xf
                                realized_pnl_by_exchange[ex_id] = float(realized_pnl_by_exchange.get(ex_id, 0.0)) + net
                                opened_iso = datetime.fromtimestamp(pos.opened_at, tz=timezone.utc).isoformat()
                                closed_ts = int(time.time())
                                closed_iso = datetime.fromtimestamp(closed_ts, tz=timezone.utc).isoformat()
                                ctx = dict(pos.entry_context or {})
                                ctx["exit_execution_exchange_id"] = ex_id
                                ctx["close_reason_rule"] = close_reason
                                await self._repository.insert_bot_trade({
                                    "trade_id": f"trade_{uuid4().hex[:12]}", "pair_symbol": pair_symbol, "session_id": active_session_id, "side": pos.side, "qty": pos.qty,
                                    "leverage": leverage, "commission_fee_pct": commission_pct, "entry_price": pos.entry_price, "exit_price": px,
                                    "entry_notional": en, "exit_notional": ex, "entry_fee": ef, "exit_fee": xf, "gross_pnl": gross, "net_pnl": net,
                                    "opened_at": opened_iso, "closed_at": closed_iso, "duration_seconds": max(0, closed_ts - pos.opened_at), "close_reason": close_reason,
                                    "decision_context": ctx,
                                    "created_at": utc_now_iso(),
                                }, instance_id=instance_id, session_id=active_session_id)
                                positions_by_exchange[ex_id] = _PaperPosition()
                                consecutive_entries = 0
                    open_positions = [p for p in positions_by_exchange.values() if p.side]
                    unrealized = 0.0
                    for ex_id, pos in positions_by_exchange.items():
                        if not pos.side:
                            continue
                        px = float((exec_snapshots.get(ex_id) or base_snapshot).get("mid_price", 0.0) or 0.0)
                        if px > 0:
                            unrealized += (px - pos.entry_price) * pos.qty * leverage if pos.side == "LONG" else (pos.entry_price - px) * pos.qty * leverage
                    position_side = f"MULTI({len(open_positions)})" if open_positions else ""
                    entry_price = float(sum(p.entry_price for p in open_positions) / len(open_positions)) if open_positions else 0.0
                    qty = float(sum(p.qty for p in open_positions)) if open_positions else 0.0
                else:
                    fill_px = mid_price
                    for ex_id in bot_execution_exchanges:
                        px = float((exec_snapshots.get(ex_id) or {}).get("mid_price", 0.0) or 0.0)
                        if px > 0:
                            fill_px = px
                            break
                    if position.side == "" and signal["action"] in {"LONG", "SHORT"} and fill_px > 0:
                        position.side = signal["action"]; position.entry_price = fill_px; position.qty = float(profile["paper_bot"]["max_position_qty"]); position.opened_at = int(time.time()); position.peak_price = fill_px; position.trough_price = fill_px
                        entry_ex = bot_execution_exchanges[0] if bot_execution_exchanges else "unknown"
                        position.entry_context = _entry_context_for(signal, base_snapshot, entry_ex)
                        trade_open_timestamps.append(time.time())
                        last_entry_ts = time.time()
                        consecutive_entries += 1
                    elif position.side and fill_px > 0:
                        pnl_pct = ((fill_px - position.entry_price) / max(position.entry_price, 1e-12)) * 100.0
                        if position.side == "SHORT":
                            pnl_pct = -pnl_pct
                        close_reason = ""
                        if take_profit_pct > 0 and pnl_pct >= take_profit_pct:
                            close_reason = "take_profit_hit"
                        elif stop_loss_pct > 0 and pnl_pct <= -stop_loss_pct:
                            close_reason = "stop_loss_hit"
                        elif signal["action"] in {"LONG", "SHORT"} and signal["action"] != position.side:
                            close_reason = "opposite_signal"
                        elif int(time.time()) - position.opened_at > int(profile["paper_bot"]["max_hold_seconds"]):
                            close_reason = "max_hold_reached"
                        should_close = bool(close_reason)
                        if should_close:
                            gross, en, ex, ef, xf = _close_pnl(position, fill_px)
                            net = gross - ef - xf
                            realized_pnl_by_exchange["combined"] = float(realized_pnl_by_exchange.get("combined", 0.0)) + net
                            realized_pnl = realized_pnl_by_exchange["combined"]
                            opened_iso = datetime.fromtimestamp(position.opened_at, tz=timezone.utc).isoformat()
                            closed_ts = int(time.time())
                            closed_iso = datetime.fromtimestamp(closed_ts, tz=timezone.utc).isoformat()
                            ctx = dict(position.entry_context or {})
                            ctx["exit_execution_exchange_id"] = (bot_execution_exchanges[0] if bot_execution_exchanges else "unknown")
                            ctx["close_reason_rule"] = close_reason
                            await self._repository.insert_bot_trade({
                                "trade_id": f"trade_{uuid4().hex[:12]}", "pair_symbol": pair_symbol, "session_id": active_session_id, "side": position.side, "qty": position.qty,
                                "leverage": leverage, "commission_fee_pct": commission_pct, "entry_price": position.entry_price, "exit_price": fill_px,
                                "entry_notional": en, "exit_notional": ex, "entry_fee": ef, "exit_fee": xf, "gross_pnl": gross, "net_pnl": net,
                                "opened_at": opened_iso, "closed_at": closed_iso, "duration_seconds": max(0, closed_ts - position.opened_at), "close_reason": close_reason,
                                "decision_context": ctx,
                                "created_at": utc_now_iso(),
                            }, instance_id=instance_id, session_id=active_session_id)
                            position = _PaperPosition()
                            consecutive_entries = 0
                    unrealized = 0.0
                    if position.side and fill_px > 0:
                        unrealized = (fill_px - position.entry_price) * position.qty * leverage if position.side == "LONG" else (position.entry_price - fill_px) * position.qty * leverage
                    position_side = position.side
                    entry_price = position.entry_price
                    qty = position.qty

                total_realized = float(sum(realized_pnl_by_exchange.values()))
                signal["data_source"] = "bot_session"
                signal["snapshot_freshness_sec"] = round(freshness_sec, 3)
                signal["active_exchange_count"] = int((snapshot or {}).get("connected_exchanges", 0) or len(signal_snapshots))
                signal["bot_signal_mode"] = bot_signal_mode
                signal["execution_position_mode"] = execution_position_mode
                signal["bot_data_exchanges"] = list(bot_data_exchanges)
                signal["execution_exchanges"] = list(bot_execution_exchanges)
                signal["loop_seq"] = loop_seq
                signal["bot_tick_ms"] = now_ts_ms
                signal["balance"] = round(initial_balance + total_realized + unrealized, 6)
                signal["realized_by_exchange"] = {k: round(float(v), 6) for k, v in realized_pnl_by_exchange.items()}
                if execution_position_mode == "separate_positions":
                    signal["open_positions_by_exchange"] = {
                        ex_id: {"side": pos.side, "entry_price": pos.entry_price, "qty": pos.qty}
                        for ex_id, pos in positions_by_exchange.items()
                        if pos.side
                    }
                elif position.side:
                    signal["open_positions_by_exchange"] = {
                        "combined": {"side": position.side, "entry_price": position.entry_price, "qty": position.qty}
                    }

                point_ts_ms = int(time.time() * 1000)
                await self._repository.insert_bot_inference_point(
                    {
                        "point_id": f"pt_{instance_id}_{point_ts_ms}",
                        "pair_symbol": pair_symbol,
                        "ts_ms": point_ts_ms,
                        "confidence": float(signal.get("confidence", 0.0) or 0.0),
                        "quality": float(signal.get("quality", 0.0) or 0.0),
                        "action": str(signal.get("action", "HOLD") or "HOLD"),
                        "reason": str(signal.get("reason", "") or ""),
                        "created_at": utc_now_iso(),
                    },
                    instance_id=instance_id,
                )

                await self._repository.upsert_bot_state(
                    pair_symbol,
                    {
                        "status": "RUNNING",
                        "position_side": position_side,
                        "active_session_id": active_session_id,
                        "entry_price": entry_price,
                        "qty": qty,
                        "unrealized_pnl": unrealized,
                        "realized_pnl": total_realized,
                        "last_signal": signal,
                    },
                    instance_id=instance_id,
                )
                await self._emit_bot_update(pair_symbol, instance_id=instance_id)
                await asyncio.sleep(1.0)
        finally:
            await self._tab_sessions.release_bot_session(instance_id)
            logger.info("Bot data session released: instance=%s pair=%s", instance_id, pair_symbol)

    def _vector_from_snapshot(self, snapshot: dict[str, Any]) -> np.ndarray:
        values = np.array([float(snapshot.get(col, 0.0) or 0.0) for col in FEATURE_COLUMNS], dtype=np.float32)
        for tool_id, columns in FEATURE_COLUMNS_BY_TOOL.items():
            if self._mode_tool_enabled("bot", tool_id):
                continue
            for idx, col in enumerate(FEATURE_COLUMNS):
                if col in columns:
                    values[idx] = 0.0
        return values

    def _normalize_vector(self, vector: np.ndarray, raw_buffer: deque[np.ndarray]) -> np.ndarray:
        stack = np.stack(raw_buffer)
        mean = stack.mean(axis=0)
        std = stack.std(axis=0)
        std = np.where(std < 1e-6, 1.0, std)
        return (vector - mean) / std

