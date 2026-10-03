import asyncio
import json
import math
import time
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.services.storage.storage_management import StorageManagementService
from app.services.storage.market_event_compaction import MarketEventCompactionService
from app.services.tool_mode_matrix import DEFAULT_TOOL_MODE_MATRIX, MODE_IDS, TOOL_IDS, normalize_tool_mode_matrix

router = APIRouter(prefix="/settings", tags=["settings"])


@router.get("")
async def get_settings(request: Request, tab_id: str = Query(default="default")) -> dict:
    settings = request.app.state.settings
    controls = _load_workload_controls(request)
    tab_symbol_overrides = getattr(request.app.state, "tab_symbol_overrides", {})
    if controls.get("live_streaming_enabled", True):
        active_symbol = await request.app.state.tab_sessions.active_symbol(tab_id)
        tab_symbol_overrides[str(tab_id or "default")] = active_symbol
        request.app.state.tab_symbol_overrides = tab_symbol_overrides
    else:
        active_symbol = str(tab_symbol_overrides.get(str(tab_id or "default"), request.app.state.symbols[0] if request.app.state.symbols else settings.binance_symbol)).upper()
    return {
        "app_env": settings.app_env,
        "symbol": active_symbol,
        "rest_base": settings.binance_rest_base,
        "ws_base": settings.binance_ws_base,
    }


@router.get("/symbols")
async def get_symbols(request: Request, tab_id: str = Query(default="default")) -> dict:
    symbols = request.app.state.symbols
    all_symbols = request.app.state.all_symbols
    if len(all_symbols) < 10:
        try:
            info = await request.app.state.rest_client.exchange_info()
            fresh_symbols: list[str] = []
            for item in info.get("symbols", []):
                if not isinstance(item, dict):
                    continue
                if item.get("status") != "TRADING":
                    continue
                if item.get("contractType") != "PERPETUAL":
                    continue
                symbol = str(item.get("symbol", "")).upper()
                if symbol:
                    fresh_symbols.append(symbol)
                    filters = item.get("filters", [])
                    for item_filter in filters:
                        if not isinstance(item_filter, dict):
                            continue
                        if item_filter.get("filterType") == "PRICE_FILTER":
                            tick_size = float(item_filter.get("tickSize", 0) or 0)
                            if tick_size > 0:
                                request.app.state.tick_sizes[symbol] = tick_size
                            break
            if fresh_symbols:
                all_symbols = sorted(set(fresh_symbols))
                request.app.state.all_symbols = all_symbols
                request.app.state.tab_sessions.update_market_metadata(all_symbols, request.app.state.tick_sizes)
        except Exception:
            pass
    controls = _load_workload_controls(request)
    tab_symbol_overrides = getattr(request.app.state, "tab_symbol_overrides", {})
    if controls.get("live_streaming_enabled", True):
        active_symbol = await request.app.state.tab_sessions.active_symbol(tab_id)
        tab_symbol_overrides[str(tab_id or "default")] = active_symbol
        request.app.state.tab_symbol_overrides = tab_symbol_overrides
    else:
        active_symbol = str(tab_symbol_overrides.get(str(tab_id or "default"), symbols[0] if symbols else request.app.state.settings.binance_symbol)).upper()
    return {
        "active_symbol": active_symbol,
        "watchlist": symbols,
        "all_symbols": all_symbols,
    }


class SymbolSwitchRequest(BaseModel):
    tab_id: str = Field(default="default", min_length=1, max_length=64)
    symbol: str = Field(min_length=3, max_length=20)


@router.post("/symbol")
async def set_symbol(request: Request, payload: SymbolSwitchRequest) -> dict:
    symbol = payload.symbol.upper()
    tab_id = payload.tab_id
    symbols = request.app.state.symbols
    all_symbols = request.app.state.all_symbols
    if not symbol.endswith("USDT") or not symbol.isalnum():
        return {
            "ok": False,
            "error": "Use BASEUSDT format, e.g. BTCUSDT / SOLUSDT",
            "active_symbol": await request.app.state.tab_sessions.active_symbol(tab_id),
        }

    controls = _load_workload_controls(request)
    if controls.get("live_streaming_enabled", True):
        result = await request.app.state.tab_sessions.switch_symbol(tab_id, symbol)
    else:
        overrides = getattr(request.app.state, "tab_symbol_overrides", {})
        overrides[tab_id] = symbol
        request.app.state.tab_symbol_overrides = overrides
        result = {
            "changed": True,
            "active_symbol": symbol,
            "symbol_resolution": {"type": "symbol_resolution", "requested_symbol": symbol, "normalized_symbol": symbol, "exchange_symbol": symbol, "supported": True},
            "source_coverage": {"type": "source_coverage_update", "symbol": symbol, "enabled_exchange_ids": [], "exchanges": [], "connected_exchanges": 0, "supported_exchanges": 0},
        }
    _save_last_symbol(request, str(result.get("active_symbol", symbol)))
    return {
        "ok": True,
        "changed": result["changed"],
        "active_symbol": result["active_symbol"],
        "watchlist": symbols,
        "all_symbols": all_symbols,
        "symbol_resolution": result["symbol_resolution"],
        "source_coverage": result["source_coverage"],
    }


class StorageDeleteRequest(BaseModel):
    targets: list[dict[str, Any]] = Field(default_factory=list)


class StorageDeleteStartResponse(BaseModel):
    job_id: str
    status: str
    total_targets: int


class RecordingPreferencePayload(BaseModel):
    pair_symbol: str = Field(min_length=3, max_length=20)
    enabled: bool = Field(default=False)


class RecordingStatePayload(BaseModel):
    active_pair_symbol: str | None = Field(default=None, min_length=3, max_length=20)
    enabled: bool = Field(default=False)


class RecordingSoftCapPayload(BaseModel):
    pair_symbol: str = Field(min_length=3, max_length=20)
    allow_continue: bool = Field(default=True)


class RecordingModePayload(BaseModel):
    mode: str = Field(default="lightweight", min_length=3, max_length=32)


class WorkloadControlsPayload(BaseModel):
    live_streaming_enabled: bool = True
    replay_enabled: bool = True
    training_enabled: bool = True
    bots_enabled: bool = True
    backfill_enabled: bool = True
    training_ram_budget_gb: float | None = None
    training_prefetch_enabled: bool = False
    training_chunk_group_size: str | int | None = "auto"
    training_process_recycle_enabled: bool = True
    training_worker_groups_before_restart: int = 1
    training_worker_memory_cap_gb: float | None = None
    keep_training_shards: bool = False


class ToolModeMatrixRowPayload(BaseModel):
    live: bool = True
    replay: bool = True
    recording: bool = True
    training: bool = True
    bot: bool = True


class ToolModeMatrixPayload(BaseModel):
    rows: dict[str, ToolModeMatrixRowPayload] = Field(default_factory=dict)


class MarketEventCompactionPayload(BaseModel):
    pair_symbol: str = Field(min_length=3, max_length=20)
    max_input_files: int = 200
    max_rows_per_output: int = 750000


DEFAULT_WORKLOAD_CONTROLS = {
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


def _normalize_training_chunk_group_size(value: Any) -> str | int:
    if value is None:
        return "auto"
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"", "auto"}:
            return "auto"
        try:
            parsed = int(text)
        except Exception:
            return "auto"
        value = parsed
    try:
        numeric = int(value)
    except Exception:
        return "auto"
    allowed = {1, 2, 3, 5, 8, 10}
    return numeric if numeric in allowed else "auto"


def _recording_preferences_path(request: Request):
    return request.app.state.settings.project_root / "config" / "recording_preferences.json"


def _recording_mode_path(request: Request):
    return request.app.state.settings.project_root / "config" / "recording_mode.json"


def _last_symbol_path(request: Request):
    return request.app.state.settings.project_root / "config" / "last_symbol.json"


def _save_last_symbol(request: Request, symbol: str) -> None:
    path = _last_symbol_path(request)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"symbol": str(symbol).upper()}, indent=2, ensure_ascii=True), encoding="utf-8")


def _normalize_recording_mode(value: str | None) -> str:
    mode = str(value or "lightweight").strip().lower()
    if mode not in {"lightweight", "full_fidelity"}:
        mode = "lightweight"
    return mode


def _load_recording_mode(request: Request) -> str:
    current = getattr(request.app.state, "recording_mode", None)
    if isinstance(current, str) and current.strip():
        return _normalize_recording_mode(current)
    path = _recording_mode_path(request)
    if not path.exists():
        return "lightweight"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return "lightweight"
    if isinstance(payload, dict):
        return _normalize_recording_mode(str(payload.get("mode", "lightweight")))
    return "lightweight"


def _save_recording_mode(request: Request, mode: str) -> None:
    normalized = _normalize_recording_mode(mode)
    path = _recording_mode_path(request)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"mode": normalized}, indent=2, ensure_ascii=True), encoding="utf-8")
    request.app.state.recording_mode = normalized


def _load_recording_preferences(request: Request) -> dict[str, bool]:
    current = getattr(request.app.state, "recording_preferences", None)
    if isinstance(current, dict):
        return {str(k).upper(): bool(v) for k, v in current.items()}
    path = _recording_preferences_path(request)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    pairs = payload.get("pairs", {}) if isinstance(payload, dict) else {}
    if not isinstance(pairs, dict):
        return {}
    return {str(k).upper(): bool(v) for k, v in pairs.items()}


def _save_recording_preferences(request: Request, pairs: dict[str, bool]) -> None:
    path = _recording_preferences_path(request)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"pairs": pairs}, indent=2, ensure_ascii=True), encoding="utf-8")
    request.app.state.recording_preferences = pairs


def _storage_delete_jobs(request: Request) -> dict[str, dict[str, Any]]:
    jobs = getattr(request.app.state, "storage_delete_jobs", None)
    if isinstance(jobs, dict):
        return jobs
    jobs = {}
    request.app.state.storage_delete_jobs = jobs
    return jobs


def _workload_controls_path(request: Request):
    return request.app.state.settings.project_root / "config" / "workload_controls.json"


def _tool_mode_matrix_path(request: Request):
    return request.app.state.settings.project_root / "config" / "tool_mode_matrix.json"


def _normalize_ram_budget_gb(value: Any) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return float(max(0.5, min(64.0, numeric)))


def _normalize_workload_controls(raw: dict[str, Any] | None) -> dict[str, Any]:
    data = raw or {}
    return {
        "live_streaming_enabled": bool(data.get("live_streaming_enabled", True)),
        "replay_enabled": bool(data.get("replay_enabled", True)),
        "training_enabled": bool(data.get("training_enabled", True)),
        "bots_enabled": bool(data.get("bots_enabled", True)),
        "backfill_enabled": bool(data.get("backfill_enabled", True)),
        "training_ram_budget_gb": _normalize_ram_budget_gb(data.get("training_ram_budget_gb")),
        "training_prefetch_enabled": bool(data.get("training_prefetch_enabled", False)),
        "training_chunk_group_size": _normalize_training_chunk_group_size(data.get("training_chunk_group_size")),
        "training_process_recycle_enabled": bool(data.get("training_process_recycle_enabled", True)),
        "training_worker_groups_before_restart": int(max(1, min(50, int(data.get("training_worker_groups_before_restart", 1) or 1)))),
        "training_worker_memory_cap_gb": _normalize_ram_budget_gb(data.get("training_worker_memory_cap_gb")),
        "keep_training_shards": bool(data.get("keep_training_shards", False)),
    }


def _load_workload_controls(request: Request) -> dict[str, Any]:
    current = getattr(request.app.state, "workload_controls", None)
    if isinstance(current, dict):
        return _normalize_workload_controls(current)
    path = _workload_controls_path(request)
    if not path.exists():
        return dict(DEFAULT_WORKLOAD_CONTROLS)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return dict(DEFAULT_WORKLOAD_CONTROLS)
    return _normalize_workload_controls(payload if isinstance(payload, dict) else None)


def _save_workload_controls(request: Request, controls: dict[str, Any]) -> None:
    normalized = _normalize_workload_controls(controls)
    path = _workload_controls_path(request)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(normalized, indent=2, ensure_ascii=True), encoding="utf-8")
    request.app.state.workload_controls = normalized


def _load_tool_mode_matrix(request: Request) -> dict[str, Any]:
    current = getattr(request.app.state, "tool_mode_matrix", None)
    if isinstance(current, dict):
        return normalize_tool_mode_matrix(current)
    path = _tool_mode_matrix_path(request)
    if not path.exists():
        return normalize_tool_mode_matrix(DEFAULT_TOOL_MODE_MATRIX)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return normalize_tool_mode_matrix(DEFAULT_TOOL_MODE_MATRIX)
    return normalize_tool_mode_matrix(payload if isinstance(payload, dict) else DEFAULT_TOOL_MODE_MATRIX)


def _save_tool_mode_matrix(request: Request, matrix: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_tool_mode_matrix(matrix)
    path = _tool_mode_matrix_path(request)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(normalized, indent=2, ensure_ascii=True), encoding="utf-8")
    request.app.state.tool_mode_matrix = normalized
    return normalized


async def _apply_workload_controls(request: Request, previous: dict[str, Any], next_controls: dict[str, Any]) -> None:
    recorder = getattr(request.app.state, "recorder", None)
    tab_sessions = getattr(request.app.state, "tab_sessions", None)
    ml_runtime = getattr(request.app.state, "ml_runtime", None)
    ml_feature_logger = getattr(request.app.state, "ml_feature_logger", None)

    prev_live = bool(previous.get("live_streaming_enabled", True))
    next_live = bool(next_controls.get("live_streaming_enabled", True))
    if prev_live and not next_live:
        if recorder is not None:
            recorder.set_recording_target(enabled=False, symbol="")
            request.app.state.recording_state = recorder.recording_state()
            await recorder.stop()
        if tab_sessions is not None:
            await tab_sessions.shutdown()
    elif (not prev_live) and next_live:
        if recorder is not None:
            await recorder.start()

    prev_training = bool(previous.get("training_enabled", True))
    next_training = bool(next_controls.get("training_enabled", True))
    if prev_training and not next_training:
        if ml_feature_logger is not None:
            await ml_feature_logger.stop()
    elif (not prev_training) and next_training:
        if ml_feature_logger is not None:
            await ml_feature_logger.start()

    prev_runtime_on = bool(previous.get("training_enabled", True) or previous.get("bots_enabled", True) or previous.get("backfill_enabled", True) or previous.get("replay_enabled", True))
    next_runtime_on = bool(next_controls.get("training_enabled", True) or next_controls.get("bots_enabled", True) or next_controls.get("backfill_enabled", True) or next_controls.get("replay_enabled", True))
    if prev_runtime_on and not next_runtime_on:
        if ml_runtime is not None:
            await ml_runtime.stop()
    elif (not prev_runtime_on) and next_runtime_on:
        if ml_runtime is not None:
            await ml_runtime.start()

    if previous.get("replay_enabled", True) and (not next_controls.get("replay_enabled", True)):
        if ml_runtime is not None:
            await ml_runtime.close_all_replays()


async def _run_storage_delete_job(request: Request, job_id: str, targets: list[dict[str, Any]]) -> None:
    jobs = _storage_delete_jobs(request)
    job = jobs.get(job_id)
    if not isinstance(job, dict):
        return
    job["status"] = "running"
    job["started_at_ms"] = int(time.time() * 1000)
    job["updated_at_ms"] = job["started_at_ms"]
    try:
        service = StorageManagementService(request.app.state.settings)
        result = await asyncio.to_thread(service.delete_targets, targets)
        now_ms = int(time.time() * 1000)
        job["status"] = "completed"
        job["completed_targets"] = int(result.get("deleted_count", 0)) + int(result.get("failed_count", 0))
        job["updated_at_ms"] = now_ms
        job["ended_at_ms"] = now_ms
        job["result"] = result
    except Exception as exc:
        now_ms = int(time.time() * 1000)
        job["status"] = "failed"
        job["updated_at_ms"] = now_ms
        job["ended_at_ms"] = now_ms
        job["error"] = str(exc)


@router.get("/storage/overview")
async def storage_overview(request: Request) -> dict[str, Any]:
    service = StorageManagementService(request.app.state.settings)
    return service.overview()


@router.post("/storage/delete")
async def storage_delete(request: Request, payload: StorageDeleteRequest) -> dict[str, Any]:
    if not payload.targets:
        raise HTTPException(status_code=400, detail="No delete targets provided.")
    service = StorageManagementService(request.app.state.settings)
    try:
        return service.delete_targets(payload.targets)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/storage/market-events/compact")
async def storage_market_events_compact(request: Request, payload: MarketEventCompactionPayload) -> dict[str, Any]:
    service = MarketEventCompactionService(request.app.state.settings.project_root)
    try:
        return await asyncio.to_thread(
            service.compact_pair,
            payload.pair_symbol,
            max_input_files=max(10, min(2000, int(payload.max_input_files))),
            max_rows_per_output=max(50_000, min(5_000_000, int(payload.max_rows_per_output))),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Compaction failed: {exc}") from exc


@router.post("/storage/delete/start", response_model=StorageDeleteStartResponse)
async def storage_delete_start(request: Request, payload: StorageDeleteRequest) -> dict[str, Any]:
    if not payload.targets:
        raise HTTPException(status_code=400, detail="No delete targets provided.")
    jobs = _storage_delete_jobs(request)
    now_ms = int(time.time() * 1000)
    job_id = f"del_{uuid.uuid4().hex[:12]}"
    jobs[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "created_at_ms": now_ms,
        "updated_at_ms": now_ms,
        "started_at_ms": None,
        "ended_at_ms": None,
        "total_targets": len(payload.targets),
        "completed_targets": 0,
        "error": "",
        "result": None,
    }
    asyncio.create_task(_run_storage_delete_job(request, job_id, payload.targets))
    return {"job_id": job_id, "status": "queued", "total_targets": len(payload.targets)}


@router.get("/storage/delete/jobs/{job_id}")
async def storage_delete_job_status(request: Request, job_id: str) -> dict[str, Any]:
    jobs = _storage_delete_jobs(request)
    job = jobs.get(job_id)
    if not isinstance(job, dict):
        raise HTTPException(status_code=404, detail="Delete job not found.")
    return job


@router.get("/recording-preferences")
async def get_recording_preferences(request: Request) -> dict[str, Any]:
    pairs = _load_recording_preferences(request)
    request.app.state.recording_preferences = pairs
    return {"pairs": pairs}


@router.get("/recording-mode")
async def get_recording_mode(request: Request) -> dict[str, Any]:
    mode = _load_recording_mode(request)
    recorder = getattr(request.app.state, "recorder", None)
    if recorder is not None:
        recorder.set_recording_mode(mode)
        mode = str(recorder.recording_mode())
        request.app.state.recording_state = recorder.recording_state()
    request.app.state.recording_mode = mode
    state = getattr(request.app.state, "recording_state", {}) or {}
    return {"mode": mode, "recording": state}


@router.get("/recording-state")
async def get_recording_state(request: Request) -> dict[str, Any]:
    recorder = getattr(request.app.state, "recorder", None)
    if recorder is not None:
        request.app.state.recording_state = recorder.recording_state()
    return {"recording": getattr(request.app.state, "recording_state", {})}


@router.post("/recording-mode")
async def set_recording_mode(request: Request, payload: RecordingModePayload) -> dict[str, Any]:
    mode = _normalize_recording_mode(payload.mode)
    recorder = getattr(request.app.state, "recorder", None)
    if recorder is not None:
        recorder.set_recording_mode(mode)
        mode = str(recorder.recording_mode())
        request.app.state.recording_state = recorder.recording_state()
    _save_recording_mode(request, mode)
    return {"ok": True, "mode": mode, "recording": getattr(request.app.state, "recording_state", {})}


@router.post("/recording-preferences")
async def set_recording_preference(request: Request, payload: RecordingPreferencePayload) -> dict[str, Any]:
    pair = payload.pair_symbol.upper().strip()
    if not pair.endswith("USDT") or not pair.isalnum():
        raise HTTPException(status_code=400, detail="Use BASEUSDT format, e.g. BTCUSDT.")
    pairs = _load_recording_preferences(request)
    pairs[pair] = bool(payload.enabled)
    _save_recording_preferences(request, pairs)
    return {"ok": True, "pair_symbol": pair, "enabled": bool(payload.enabled), "pairs": pairs}


@router.post("/recording-state")
async def set_recording_state(request: Request, payload: RecordingStatePayload) -> dict[str, Any]:
    recorder = getattr(request.app.state, "recorder", None)
    if recorder is None:
        raise HTTPException(status_code=503, detail="Recorder is not available.")
    symbol = (payload.active_pair_symbol or "").upper().strip()
    enabled = bool(payload.enabled and symbol)
    recorder.set_recording_target(enabled=enabled, symbol=symbol if enabled else "")
    state = recorder.recording_state()
    request.app.state.recording_state = state
    return {"ok": True, "recording": state}


@router.post("/recording-soft-cap")
async def set_recording_soft_cap(request: Request, payload: RecordingSoftCapPayload) -> dict[str, Any]:
    recorder = getattr(request.app.state, "recorder", None)
    if recorder is None:
        raise HTTPException(status_code=503, detail="Recorder is not available.")
    pair = str(payload.pair_symbol or "").upper().strip()
    if not pair.endswith("USDT") or not pair.isalnum():
        raise HTTPException(status_code=400, detail="Use BASEUSDT format, e.g. BTCUSDT.")
    state = recorder.set_soft_cap_override(pair, allow_continue=bool(payload.allow_continue))
    request.app.state.recording_state = state
    return {
        "ok": True,
        "pair_symbol": pair,
        "allow_continue": bool(payload.allow_continue),
        "recording": state,
    }


@router.get("/workload-controls")
async def get_workload_controls(request: Request) -> dict[str, Any]:
    controls = _load_workload_controls(request)
    request.app.state.workload_controls = controls
    return controls


@router.get("/tool-mode-matrix")
async def get_tool_mode_matrix(request: Request) -> dict[str, Any]:
    matrix = _load_tool_mode_matrix(request)
    request.app.state.tool_mode_matrix = matrix
    return matrix


@router.post("/tool-mode-matrix")
async def set_tool_mode_matrix(request: Request, payload: ToolModeMatrixPayload) -> dict[str, Any]:
    current = _load_tool_mode_matrix(request)
    rows = dict(current.get("rows", {}))
    incoming = payload.model_dump().get("rows", {})
    if isinstance(incoming, dict):
        for tool_id in TOOL_IDS:
            row_raw = incoming.get(tool_id, {})
            if not isinstance(row_raw, dict):
                continue
            next_row = dict(rows.get(tool_id, {}))
            for mode_id in MODE_IDS:
                if mode_id in row_raw:
                    next_row[mode_id] = bool(row_raw.get(mode_id))
            rows[tool_id] = next_row
    saved = _save_tool_mode_matrix(request, {"version": 1, "rows": rows})
    return {"ok": True, **saved}


@router.post("/workload-controls")
async def set_workload_controls(request: Request, payload: WorkloadControlsPayload) -> dict[str, Any]:
    previous = _load_workload_controls(request)
    next_controls = _normalize_workload_controls(payload.model_dump())
    await _apply_workload_controls(request, previous, next_controls)
    _save_workload_controls(request, next_controls)
    return {"ok": True, "workload_controls": next_controls}
