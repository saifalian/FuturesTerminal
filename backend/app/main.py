from __future__ import annotations

import asyncio
import logging
import math
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI

from app.api.rest_server import create_app
from app.api.ws_server import BroadcastHub
from app.config import load_json_config, load_settings
from app.logging_conf import configure_logging
from app.services.binance.rest_client import BinanceRestClient
from app.services.market.tab_session_manager import TabSessionManager
from app.services.ml.feature_logger import MlFeatureLogger
from app.services.ml.repository import MlRepository
from app.services.ml.runtime_manager import MlRuntimeManager
from app.services.storage.recorder import RawEventRecorder
from app.services.tool_mode_matrix import DEFAULT_TOOL_MODE_MATRIX, normalize_tool_mode_matrix

logger = logging.getLogger(__name__)


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


async def load_exchange_metadata(rest_client: BinanceRestClient) -> tuple[list[str], dict[str, float]]:
    info: dict | None = None
    for attempt in range(1, 4):
        try:
            info = await rest_client.exchange_info()
            break
        except Exception as exc:
            # Network/DNS glitches are common on startup; retry quickly before fallback.
            if attempt < 3:
                await asyncio.sleep(0.8 * attempt)
                continue
            logger.warning(
                "Could not fetch exchange symbols after %d attempts (%s: %s)",
                attempt,
                type(exc).__name__,
                exc,
            )
            return [], {}

    raw_symbols = info.get("symbols", []) if isinstance(info, dict) else []
    symbols: list[str] = []
    tick_sizes: dict[str, float] = {}
    for item in raw_symbols:
        if not isinstance(item, dict):
            continue
        if item.get("status") != "TRADING":
            continue
        if item.get("contractType") == "PERPETUAL":
            symbol = str(item.get("symbol", "")).upper()
            if symbol:
                symbols.append(symbol)
                filters = item.get("filters", [])
                for item_filter in filters:
                    if not isinstance(item_filter, dict):
                        continue
                    if item_filter.get("filterType") == "PRICE_FILTER":
                        tick_size = float(item_filter.get("tickSize", 0) or 0)
                        if tick_size > 0:
                            tick_sizes[symbol] = tick_size
                        break
    return sorted(set(symbols)), tick_sizes


async def bootstrap() -> tuple:
    settings = load_settings()
    configure_logging(settings.log_level, settings.project_root / "backend" / "data" / "logs")

    state_holder: dict[str, Any] = {}

    def _tool_mode_matrix_resolver() -> dict:
        app_ref = state_holder.get("app")
        if app_ref is None:
            return normalize_tool_mode_matrix(DEFAULT_TOOL_MODE_MATRIX)
        matrix = getattr(app_ref.state, "tool_mode_matrix", None)
        return normalize_tool_mode_matrix(matrix if isinstance(matrix, dict) else DEFAULT_TOOL_MODE_MATRIX)

    def _workload_controls_resolver() -> dict:
        app_ref = state_holder.get("app")
        if app_ref is None:
            return {
                "live_streaming_enabled": True,
                "replay_enabled": True,
                "training_enabled": True,
                "bots_enabled": True,
                "backfill_enabled": True,
                "training_ram_budget_gb": None,
                "training_prefetch_enabled": False,
                "training_chunk_group_size": "auto",
                "training_process_recycle_enabled": True,
                "training_worker_groups_before_restart": 1,
                "training_worker_memory_cap_gb": None,
                "keep_training_shards": False,
            }
        controls = getattr(app_ref.state, "workload_controls", None)
        return controls if isinstance(controls, dict) else {}

    hub = BroadcastHub()
    rest_client = BinanceRestClient(
        base_url=settings.binance_rest_base,
        api_key=settings.binance_api_key,
        api_secret=settings.binance_api_secret,
    )
    recorder = RawEventRecorder(
        sqlite_path=settings.sqlite_path,
        replay_dir=settings.replay_dir,
        market_events_dir=settings.market_events_dir,
        schema_path=settings.project_root / "backend" / "app" / "services" / "storage" / "schema.sql",
        pair_soft_cap_bytes=int(settings.market_pair_soft_cap_gb * (1024**3)),
        tool_mode_matrix_resolver=_tool_mode_matrix_resolver,
    )
    symbols_cfg = load_json_config(settings, "symbols.json")
    last_symbol_cfg = load_json_config(settings, "last_symbol.json")
    last_symbol = str(last_symbol_cfg.get("symbol", "") if isinstance(last_symbol_cfg, dict) else "").upper()
    watchlist = symbols_cfg.get("watchlist", [])
    symbols = [str(item).upper() for item in watchlist if str(item).strip()]
    if settings.binance_symbol not in symbols:
        symbols.append(settings.binance_symbol)
    all_symbols, tick_sizes = await load_exchange_metadata(rest_client)
    if not all_symbols:
        all_symbols = sorted(set(symbols))
    if settings.binance_symbol not in all_symbols:
        all_symbols.append(settings.binance_symbol)

    default_symbol = settings.binance_symbol
    if symbols:
        default_symbol = symbols[0]
    if last_symbol and last_symbol in all_symbols:
        default_symbol = last_symbol

    recording_mode_cfg = load_json_config(settings, "recording_mode.json")
    recording_mode = str(recording_mode_cfg.get("mode", "lightweight") if isinstance(recording_mode_cfg, dict) else "lightweight")
    recorder.set_recording_mode(recording_mode)

    await recorder.start()
    recorder.set_recording_target(enabled=False, symbol="")
    tab_sessions = TabSessionManager(
        ws_base=settings.binance_ws_base,
        rest_client=rest_client,
        hub=hub,
        recorder=recorder,
        all_symbols=all_symbols,
        tick_sizes=tick_sizes,
        default_symbol=default_symbol,
        selection_path=settings.project_root / "config" / "exchange-selection.json",
        tool_mode_matrix_resolver=_tool_mode_matrix_resolver,
    )
    ml_repository = MlRepository(
        sqlite_path=settings.ml_sqlite_path,
        schema_path=settings.project_root / "backend" / "app" / "services" / "ml" / "schema.sql",
    )
    await ml_repository.start()
    ml_feature_logger = MlFeatureLogger(
        tab_sessions=tab_sessions,
        repository=ml_repository,
        features_root=settings.ml_features_dir,
        retention_days=settings.ml_retention_days,
    )
    await ml_feature_logger.start()
    ml_runtime = MlRuntimeManager(
        repository=ml_repository,
        tab_sessions=tab_sessions,
        hub=hub,
        models_root=settings.models_root,
        feature_data_root=settings.ml_features_dir,
        terminal_sqlite_path=settings.sqlite_path,
        market_events_root=settings.market_events_dir,
        tool_mode_matrix_resolver=_tool_mode_matrix_resolver,
        workload_controls_resolver=_workload_controls_resolver,
    )
    await ml_runtime.start()
    logger.info("Reporting schema version: trade_outcome_v2")

    @asynccontextmanager
    async def _lifespan(_: FastAPI):
        yield
        logger.info("Shutdown requested")
        await ml_runtime.stop()
        await ml_feature_logger.stop()
        await ml_repository.stop()
        await tab_sessions.shutdown()
        await recorder.stop()
        await rest_client.close()

    app = create_app(settings, hub, lifespan=_lifespan)
    state_holder["app"] = app
    app.state.recorder = recorder
    app.state.tab_sessions = tab_sessions
    app.state.symbols = symbols
    app.state.all_symbols = all_symbols
    app.state.tick_sizes = tick_sizes
    app.state.rest_client = rest_client
    app.state.ml_repository = ml_repository
    app.state.ml_feature_logger = ml_feature_logger
    app.state.ml_runtime = ml_runtime
    recording_cfg = load_json_config(settings, "recording_preferences.json")
    pairs_raw = recording_cfg.get("pairs", {}) if isinstance(recording_cfg, dict) else {}
    app.state.recording_preferences = (
        {str(k).upper(): bool(v) for k, v in pairs_raw.items()} if isinstance(pairs_raw, dict) else {}
    )
    app.state.recording_state = recorder.recording_state()
    app.state.recording_mode = recorder.recording_mode()
    app.state.recording_state = recorder.recording_state()
    workload_cfg = load_json_config(settings, "workload_controls.json")
    app.state.workload_controls = {
        "live_streaming_enabled": bool(workload_cfg.get("live_streaming_enabled", True)),
        "replay_enabled": bool(workload_cfg.get("replay_enabled", True)),
        "training_enabled": bool(workload_cfg.get("training_enabled", True)),
        "bots_enabled": bool(workload_cfg.get("bots_enabled", True)),
        "backfill_enabled": bool(workload_cfg.get("backfill_enabled", True)),
        "training_ram_budget_gb": _normalize_training_ram_budget_gb(workload_cfg.get("training_ram_budget_gb")),
        "training_prefetch_enabled": bool(workload_cfg.get("training_prefetch_enabled", False)),
        "training_chunk_group_size": workload_cfg.get("training_chunk_group_size", "auto"),
        "training_process_recycle_enabled": bool(workload_cfg.get("training_process_recycle_enabled", True)),
        "training_worker_groups_before_restart": int(max(1, min(50, int(workload_cfg.get("training_worker_groups_before_restart", 1) or 1)))),
        "training_worker_memory_cap_gb": _normalize_training_ram_budget_gb(workload_cfg.get("training_worker_memory_cap_gb")),
        "keep_training_shards": bool(workload_cfg.get("keep_training_shards", False)),
    }
    tool_matrix_cfg = load_json_config(settings, "tool_mode_matrix.json")
    app.state.tool_mode_matrix = normalize_tool_mode_matrix(tool_matrix_cfg if isinstance(tool_matrix_cfg, dict) else DEFAULT_TOOL_MODE_MATRIX)
    app.state.tab_symbol_overrides = {}
    controls = app.state.workload_controls
    if not controls["live_streaming_enabled"]:
        recorder.set_recording_target(enabled=False, symbol="")
        app.state.recording_state = recorder.recording_state()
        await recorder.stop()
        await tab_sessions.shutdown()
    if not controls["training_enabled"]:
        await ml_feature_logger.stop()
    runtime_on = controls["training_enabled"] or controls["bots_enabled"] or controls["backfill_enabled"] or controls["replay_enabled"]
    if not runtime_on:
        await ml_runtime.stop()
    elif not controls["replay_enabled"]:
        await ml_runtime.close_all_replays()

    return app, settings


async def run() -> None:
    app, settings = await bootstrap()
    config = uvicorn.Config(
        app=app,
        host=settings.app_host,
        port=settings.app_port,
        reload=False,
        use_colors=False,
    )
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(run())
